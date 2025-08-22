module Foobara
  class PostgresqlCrudDriver < Persistence::EntityAttributesCrudDriver
    class NoDatabaseUrlError < StandardError; end
    class NoSuchColumnOrTableError < StandardError; end

    class UnsupportedPgColumnTypeError < StandardError
      def initialize(pg_type, attribute_name, entity_class)
        # :nocov:
        super("Unsupported column type #{pg_type} for attribute #{attribute_name} on #{entity_class.entity_name}")
        # :nocov:
      end
    end

    class << self
      def get_transaction_number
        @get_transaction_number ||= 0
        @get_transaction_number += 1
        if @get_transaction_number > 65_535
          # TODO: test this codepath somehow
          # :nocov:
          @get_transaction_number = 1
          # :nocov:
        end
        @get_transaction_number
      end

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

    attr_accessor :creds, :transaction_number

    # We intentionally don't call super because super would open a connection which we do not want
    # since we want one connection per transaction.
    # rubocop:disable Lint/MissingSuper
    def initialize(connection_or_credentials = nil)
      self.transaction_number = self.class.get_transaction_number
      self.tables = {}
      self.creds = case connection_or_credentials
                   when ::String, ::Hash
                     connection_or_credentials
                   when nil
                     ENV["DATABASE_URL"] || raise(
                       NoDatabaseUrlError,
                       'Must set ENV["DATABASE_URL"] if trying to initialize PostgresqlCrudDriver with no arguments'
                     )
                   else
                     # :nocov:
                     raise ArgumentError, "Expected a String or Hash with connection creds or nil with DATABASE_URL set"
                     # :nocov:
                   end
    end
    # rubocop:enable Lint/MissingSuper

    def open_connection
      PG.connect(creds)
    end

    def connection_pool
      @connection_pool ||= ConnectionPool.new(max_connections: 5) { open_connection }
    end

    def open_transaction
      connection = connection_pool.checkout
      tx = PgTransaction.new(connection)
      connection.exec("BEGIN")
      flush_transaction(tx)
      tx
    end

    def flush_transaction(raw_tx)
      raw_tx.connection.exec("SAVEPOINT foobara_crud_driver_revert_point_#{transaction_number}")
    end

    def revert_transaction(raw_tx)
      raw_tx.connection.exec("ROLLBACK TO foobara_crud_driver_revert_point_#{transaction_number}")
    end

    def rollback_transaction(raw_tx)
      raw_tx.connection.exec("ROLLBACK")
      connection_pool.checkin(raw_tx.connection)
    end

    def commit_transaction(raw_tx)
      raw_tx.connection.exec("COMMIT")
      connection_pool.checkin(raw_tx.connection)
    end

    class Table < Persistence::EntityAttributesCrudDriver::Table
      ARRAY_ELEMENT_ENCODERS = {
        "_int4" => PG::TextEncoder::Integer.new,
        "_int8" => PG::TextEncoder::Integer.new,
        "_text" => PG::TextEncoder::String.new,
        "_varchar" => PG::TextEncoder::String.new,
        "_bool" => PG::TextEncoder::Boolean.new,
        "_float4" => PG::TextEncoder::Float.new,
        "_float8" => PG::TextEncoder::Float.new
      }.freeze

      def all(page_size: nil)
        Enumerator.new do |yielder|
          after = nil

          loop do
            page = fetch_page(after:, page_size:)

            break if page.empty?

            page.each do |record|
              yielder << record
            end

            after = DataPath.value_at(entity_class.primary_key_attribute, page.last)
          end
        end.lazy
      end

      def fetch_page(after: nil, page_size: nil, order_by: entity_class.primary_key_attribute)
        page_size ||= 100

        sql = <<~SQL
          SELECT *
          FROM #{PostgresqlCrudDriver.escape_identifier(table_name)}
        SQL

        if after
          column, value = normalize_attribute(order_by, after)
          sql += " WHERE #{column} > #{value} "
        else
          column = PostgresqlCrudDriver.escape_identifier(order_by)
        end

        sql += <<~SQL
          ORDER BY #{column}
          LIMIT #{page_size}
        SQL

        result_data = raw_connection.exec(sql)

        result_data.map do |attributes|
          restore_pg_attributes(attributes)
        end
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
      rescue PG::UniqueViolation => e
        raise CannotInsertError.new(record_id, "already exists: #{e.message}")
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

        attributes = raw_connection.exec(sql).first

        restore_pg_attributes(attributes)
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

      def restore_pg_attributes(attributes)
        return attributes if attributes.nil?

        attributes = attributes.to_h do |attribute_name, value|
          info = column_info[attribute_name.to_s]

          value = case info[:type]
                  when "integer", "text", "timestamp without time zone"
                    value
                  when "jsonb"
                    if value.nil?
                      unless info[:is_nullable]
                        # :nocov:
                        raise "Unexpected nil value for #{attribute_name}"
                        # :nocov:
                      end

                      nil
                    else
                      JSON.parse(value)
                    end
                  when "ARRAY"
                    decoder = PG::TextDecoder::Array.new(name: info[:element_type])
                    decoder.decode(value)
                  else
                    # :nocov:
                    raise UnsupportedPgColumnTypeError.new(info[:type], attribute_name, entity_class)
                    # :nocov:
                  end

          [attribute_name, value]
        end

        # TODO: don't do this? Is this a higher-up responsibility?
        entity_class.attributes_type.process_value!(attributes)
      end

      def normalize_attribute(attribute_name, value)
        info = column_info[attribute_name.to_s]

        unless info
          # :nocov:
          raise NoSuchColumnOrTableError, "Either #{table_name} or #{table_name}.#{attribute_name} does not exist"
          # :nocov:
        end

        pg_type = info[:type]
        foobara_type = entity_class.model_type.element_types.element_types[attribute_name]

        value = if value.nil?
                  if info[:is_nullable]
                    "NULL"
                  else
                    # :nocov:
                    raise "Unexpected nil value for #{attribute_name}"
                    # :nocov:
                  end
                elsif foobara_type.extends?(:number)
                  case pg_type
                  when "integer"
                    value.to_i
                  else
                    # :nocov:
                    raise UnsupportedPgColumnTypeError.new(pg_type, attribute_name, entity_class)
                    # :nocov:
                  end
                elsif foobara_type.extends?(:string) || foobara_type.extends?(:symbol)
                  case pg_type
                  when "text"
                    "'#{PG::Connection.escape(value.to_s)}'"
                  else
                    # :nocov:
                    raise UnsupportedPgColumnTypeError.new(pg_type, attribute_name, entity_class)
                    # :nocov:
                  end
                elsif foobara_type.extends?(:datetime)
                  case pg_type
                  when "timestamp without time zone"
                    "'#{PG::Connection.escape(value.inspect)}'"
                  else
                    # :nocov:
                    raise UnsupportedPgColumnTypeError.new(pg_type, attribute_name, entity_class)
                    # :nocov:
                  end
                elsif foobara_type.extends?(:model) || foobara_type.extends?(:attributes)
                  case pg_type
                  when "jsonb"
                    "'#{PG::Connection.escape(JSON.fast_generate(value))}'"
                  else
                    # :nocov:
                    raise UnsupportedPgColumnTypeError.new(pg_type, attribute_name, entity_class)
                    # :nocov:
                  end
                elsif foobara_type.extends?(:array)
                  element_type = foobara_type.element_type

                  if element_type.extends?(:detached_entity)
                    case pg_type
                    when "ARRAY"
                      elements_type = ARRAY_ELEMENT_ENCODERS[info[:element_type]]
                      array_string = PG::TextEncoder::Array.new(elements_type:).encode(value)
                      escaped = PG::Connection.escape(array_string)

                      "'#{escaped}'"
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
                else
                  # :nocov:
                  raise UnsupportedPgColumnTypeError.new(pg_type, attribute_name, entity_class)
                  # :nocov:
                end

        [PostgresqlCrudDriver.escape_identifier(attribute_name), value]
      end

      def column_info
        return @column_info if @column_info

        sql = <<~HERE
          SELECT column_name, data_type, is_nullable, udt_name
          FROM information_schema.columns
          WHERE table_schema = 'public' AND table_name = '#{raw_connection.escape(table_name)}'
          ORDER BY ordinal_position
        HERE

        result = raw_connection.exec(sql)

        @column_info = result.to_h do |row|
          [
            row["column_name"],
            {
              type: row["data_type"],
              is_nullable: row["is_nullable"] == "YES",
              element_type: row["udt_name"]
            }
          ]
        end
      end
    end
  end
end
