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
      attr_writer :pg

      def pg
        @pg ||= if ENV["DATABASE_URL"]
                  PG.connect(ENV["DATABASE_URL"])
                else
                  raise NoDatabaseUrlError,
                        'Must set ENV["DATABASE_URL"] if trying to initialize PostgresqlCrudDriver with no arguments'
                end
      end

      def escape_identifier(identifier)
        if identifier.is_a?(::Symbol)
          identifier = identifier.to_s
        end

        "\"#{PG::Connection.escape(identifier).gsub('"', '""')}\""
      end
    end

    def open_connection(connection_url_or_credentials)
      case connection_url_or_credentials
      when PG::Connection
        connection_url_or_credentials
      when ::String, ::Hash
        PG.connect(connection_url_or_credentials)
      when nil
        self.class.pg
      end
    end

    class Table < Persistence::EntityAttributesCrudDriver::Table
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

        attributes.merge(
          entity_class.primary_key_attribute => record_data.first[entity_class.primary_key_attribute.to_s]
        )
      end

      def count
        sql = <<~SQL
          SELECT COUNT(*)
          FROM #{PostgresqlCrudDriver.escape_identifier(table_name)}
        SQL

        raw_connection.exec(sql).first["count"].to_i
      end

      private

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
