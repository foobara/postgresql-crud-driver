require "connection_pool"

module Foobara
  class PostgresqlCrudDriver < Persistence::EntityAttributesCrudDriver
    class NoDatabaseUrlError < StandardError; end

    class UnsupportedPgColumnTypeError < StandardError
      def initialize(pg_type, attribute_name, entity_class)
        # :nocov:
        super("Unsupported column type #{pg_type} for attribute #{attribute_name} on #{entity_class.entity_name}")
        # :nocov:
      end
    end

    class << self
      def has_real_transactions?
        true
      end

      def escape_identifier(identifier)
        if identifier.is_a?(::Symbol)
          identifier = identifier.to_s
        end

        "\"#{PG::Connection.escape(identifier).gsub('"', '""')}\""
      end
    end

    attr_accessor :creds

    # We intentionally don't call super because super would open a connection which we do not want
    # since we want one connection per transaction.
    # rubocop:disable Lint/MissingSuper
    def initialize(connection_or_credentials = nil)
      self.tables = {}
      self.creds = case connection_or_credentials
                   when ::String, ::Hash
                     connection_or_credentials
                   when nil
                     ENV["DATABASE_URL"] || raise(NoDatabaseUrlError,
                                                  'Must set ENV["DATABASE_URL"] if trying to initialize PostgresqlCrudDriver with no arguments')
                   else
                     raise ArgumentError, "Expected a String or Hash with connection creds or nil with DATABASE_URL set"
                   end
    end
    # rubocop:enable Lint/MissingSuper

    def open_connection
      PG.connect(creds)
    end

    def connection_pool
      @connection_pool ||= ConnectionPool.new(size: 5, timeout: 5) { open_connection }
    end

    def open_transaction
      connection = connection_pool.checkout
      tx = PgTransaction.new(connection)
      connection.exec("BEGIN")
      flush_transaction(tx)
      tx
    end

    def flush_transaction(raw_tx)
      raw_tx.connection.exec("SAVEPOINT foobara_crud_driver_revert_point")
    end

    def revert_transaction(raw_tx)
      raw_tx.connection.exec("ROLLBACK TO foobara_crud_driver_revert_point")
    end

    def rollback_transaction(raw_tx)
      raw_tx.connection.exec("ROLLBACK")
      connection_pool.checkin
    end

    def commit_transaction(raw_tx)
      raw_tx.connection.exec("COMMIT")
      connection_pool.checkin
    end

    class Table < Persistence::EntityAttributesCrudDriver::Table
      def all(page_size: nil)
        Enumerator.new do |yielder|
          after = nil

          loop do
            page = fetch_page(after:, page_size:)

            break if page.empty?

            page.each do |record|
              yielder << record
            end

            after = page.last[entity_class.primary_key_attribute.to_s]
          end
        end.lazy
      end

      def fetch_page(after: nil, page_size: nil, order_by: entity_class.primary_key_attribute)
        page_size ||= 100

        column, value = normalize_attribute(order_by, after)

        sql = <<~SQL
          SELECT *
          FROM #{PostgresqlCrudDriver.escape_identifier(table_name)}
        SQL

        if after
          sql += " WHERE #{column} > #{value} "
        end

        sql += <<~SQL
          ORDER BY #{column}
          LIMIT #{page_size}
        SQL

        result_data = raw_connection.exec(sql)

        result_data.to_a
      end

      def insert(attributes)
        columns = []
        values = []

        attributes.each_pair do |attribute_name, value|
          column_name, value = normalize_attribute(attribute_name, value)

          columns << column_name
          values << value
        end

        sql = <<~SQL
          INSERT INTO #{PostgresqlCrudDriver.escape_identifier(table_name)}
          (#{columns.join(", ")})
          VALUES (#{values.join(", ")})
          RETURNING #{PostgresqlCrudDriver.escape_identifier(entity_class.primary_key_attribute)}
        SQL

        record_data = raw_connection.exec(sql)
        record_id = record_data.first[entity_class.primary_key_attribute.to_s]

        find(record_id)
      end

      def count
        sql = <<~SQL
          SELECT COUNT(*)
          FROM #{PostgresqlCrudDriver.escape_identifier(table_name)}
        SQL

        raw_connection.exec(sql).first["count"].to_i
      end

      def find(record_id)
        column, value = normalize_attribute(entity_class.primary_key_attribute, record_id)

        sql = <<~SQL
          SELECT *
          FROM #{PostgresqlCrudDriver.escape_identifier(table_name)}
          WHERE #{column} = #{value}
        SQL

        raw_connection.exec(sql).first
      end

      def update(attributes)
        record_id = record_id_for(attributes)

        unless exists?(record_id)
          # :nocov:
          raise CannotUpdateError.new(record_id, "does not exist")
          # :nocov:
        end

        set_expressions = []

        attributes.each_pair do |attribute_name, value|
          column_name, value = normalize_attribute(attribute_name, value)

          set_expressions << "#{column_name} = #{value}"
        end

        primary_key, record_id = normalize_attribute(entity_class.primary_key_attribute, record_id)

        sql = <<~SQL
          UPDATE #{PostgresqlCrudDriver.escape_identifier(table_name)}
          SET #{set_expressions.join(", ")}
          WHERE #{primary_key} = #{record_id}
        SQL

        raw_connection.exec(sql)
        find(record_id)
      end

      def hard_delete(record_id)
        unless exists?(record_id)
          # :nocov:
          raise CannotDeleteError.new(record_id, "does not exist")
          # :nocov:
        end

        primary_key, record_id = normalize_attribute(entity_class.primary_key_attribute, record_id)

        sql = <<~SQL
          DELETE FROM #{PostgresqlCrudDriver.escape_identifier(table_name)}
          WHERE #{primary_key} = #{record_id}
        SQL

        raw_connection.exec(sql)
      end

      def hard_delete_all
        sql = <<~SQL
          DELETE FROM #{PostgresqlCrudDriver.escape_identifier(table_name)}
        SQL

        raw_connection.exec(sql)
      end

      private

      def raw_connection
        # Feels so weird to grab the transaction this way, hmmm
        entity_class.current_transaction.raw_tx.connection
      end

      def normalize_attribute(attribute_name, value)
        pg_type = column_types[attribute_name.to_s]
        foobara_type = entity_class.model_type.element_types.element_types[attribute_name]

        value = if foobara_type.extends?(:number)
                  case pg_type
                  when "integer"
                    value
                  else
                    # :nocov:
                    raise UnsupportedPgColumnTypeError.new(pg_type, attribute_name, entity_class)
                    # :nocov:
                  end
                elsif foobara_type.extends?(:string) || foobara_type.extends?(:symbol)
                  case pg_type
                  when "text"
                    "'#{value}'"
                  else
                    # :nocov:
                    raise UnsupportedPgColumnTypeError.new(pg_type, attribute_name, entity_class)
                    # :nocov:
                  end
                elsif foobara_type.extends?(:datetime)
                  case pg_type
                  when "timestamp without time zone"
                    "'#{value}'"
                  else
                    # :nocov:
                    raise UnsupportedPgColumnTypeError.new(pg_type, attribute_name, entity_class)
                    # :nocov:
                  end
                else
                  # :nocov:
                  raise UnsupportedPgColumnTypeError.new(pg_type, attribute_name, entity_class)
                  # :nocov:
                end

        [PostgresqlCrudDriver.escape_identifier(attribute_name), value]
      end

      def column_types
        sql = <<~HERE
          SELECT column_name, data_type
          FROM information_schema.columns
          WHERE table_schema = 'public' AND table_name = '#{raw_connection.escape(table_name)}'
          ORDER BY ordinal_position
        HERE

        result = raw_connection.exec(sql)

        result.to_h { |row| [row["column_name"], row["data_type"]] }
      end
    end
  end
end
