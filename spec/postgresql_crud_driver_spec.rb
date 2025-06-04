RSpec.describe Foobara::PostgresqlCrudDriver do
  let(:crud_driver) { described_class.new }

  let(:setup_connection) do
    PG::Connection.new("postgres://#{db_user}:#{db_password}@localhost:5432/postgres")
  end
  let(:pg) { described_class.pg }
  let(:db_name) { "foobara_test" }
  let(:db_user) { "testuser" }
  let(:db_password) { "testpassword" }
  let(:entity_class) do
    stub_class("SomeEntity", Foobara::Entity) do
      attributes id: :integer,
                 foo: :integer,
                 bar: :symbol,
                 created_at: :datetime

      primary_key :id
    end
  end
  let(:skip_setting_up_database) { false }
  let(:credentials) { nil }
  let(:database_url) do
    "postgres://#{db_user}:#{db_password}@localhost:5432/#{db_name}"
  end

  after do
    unless skip_setting_up_database
      pg.close
    end
    Foobara.reset_alls
    described_class.reset_all
  end

  before do
    stub_env_var("DATABASE_URL", database_url)

    unless skip_setting_up_database
      setup_connection.exec("DROP DATABASE IF EXISTS #{described_class.escape_identifier(db_name)}")
      setup_connection.exec("CREATE DATABASE #{described_class.escape_identifier(db_name)}")
      pg.exec("CREATE TABLE some_entity (
        id SERIAL PRIMARY KEY,
        foo INTEGER,
        bar TEXT,
        created_at TIMESTAMP
      );")

      Foobara::Persistence.default_crud_driver = described_class.new(credentials)
    end
  end

  describe ".pg" do
    context "with no DATABASE_URL env var" do
      let(:skip_setting_up_database) { true }
      let(:database_url) { nil }

      it "raises" do
        expect {
          described_class.pg
        }.to raise_error(described_class::NoDatabaseUrlError)
      end
    end

    context "with DATABASE_URL env var" do
      it "uses DATABASE_URL to connect" do
        expect(pg).to be_a(PG::Connection)

        expect(pg.host).to eq("localhost")
        expect(pg.port).to eq(5432)
        expect(pg.db).to eq("foobara_test")
        expect(pg.user).to eq("testuser")
      end
    end
  end

  describe "#initialize" do
    context "with an existing connection" do
      let(:existing_connection) { PG::Connection.new(database_url) }
      let(:crud_driver) do
        described_class.new(existing_connection)
      end

      after do
        existing_connection.close
      end

      it "uses the existing connection" do
        expect(crud_driver.raw_connection).to be(existing_connection)
      end
    end

    context "with a database url" do
      let(:crud_driver) { described_class.new(database_url) }

      it "connects to the database" do
        expect(crud_driver.raw_connection).to be_a(PG::Connection)
        expect(crud_driver.raw_connection.host).to eq("localhost")
      end
    end
  end

  describe "#insert" do
    it "inserts a record" do
      expect {
        entity_class.transaction do
          entity_class.create(foo: 1, bar: :foo)
        end
      }.to change {
        pg.exec("SELECT COUNT(*) FROM some_entity").first["count"].to_i
      }.from(0).to(1)
    end
  end
end
