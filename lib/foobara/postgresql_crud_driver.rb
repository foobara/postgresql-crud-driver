require "foobara/all"
require "pg"

module Foobara
  class PostgresqlCrudDriver < Persistence::EntityAttributesCrudDriver
    class << self
      def reset_all
        if instance_variable_defined?(:@pg)
          # TODO: protect against this in production
          remove_instance_variable(:@pg)
        end
      end
    end
  end
end

Foobara::Util.require_directory "#{__dir__}/../../src"
