module Foobara
  class PostgresqlCrudDriver < Persistence::EntityAttributesCrudDriver
    class PgTransaction
      attr_accessor :connection

      def initialize(connection)
        self.connection = connection
      end
    end
  end
end
