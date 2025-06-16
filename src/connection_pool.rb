module Foobara
  class PostgresqlCrudDriver < Persistence::EntityAttributesCrudDriver
    class ConnectionPool
      class TooManyConnectionsError < StandardError; end

      attr_accessor :max_connections, :connection_proc, :available_connections, :in_use_connections

      def initialize(max_connections: 5, &connection_proc)
        self.max_connections = max_connections
        self.connection_proc = connection_proc
        self.available_connections = []
        self.in_use_connections = []
      end

      def checkout
        connection = available_connections.pop

        unless connection
          if in_use_connections.size >= max_connections
            # :nocov:
            raise TooManyConnectionsError, "#{in_use_connections.size} connections in use, cannot allocate more."
            # :nocov:
          end

          connection = connection_proc.call
        end

        in_use_connections << connection

        connection
      end

      def checkin(connection)
        in_use_connections.delete(connection)
        available_connections << connection
      end

      def clear(&)
        available_connections.each(&)
        in_use_connections.each(&)
        available_connections.clear
        in_use_connections.clear
      end
    end
  end
end
