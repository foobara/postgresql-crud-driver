require "foobara/spec_helpers/it_behaves_like_a_crud_driver"

RSpec.describe Foobara::PostgresqlCrudDriver do
  it_behaves_like_a_crud_driver

  let(:crud_driver) { described_class.new(creds) }
  let(:creds) { database_url }
  let(:setup_connection) { PG::Connection.new(admin_database_url) }
  let(:db_name) { "foobara_test" }
  let(:db_user) { "testuser" }
  let(:db_password) { "testpassword" }
  let(:skip_setting_up_database) { false }
  let(:database_url_without_db) { "postgres://#{db_user}:#{db_password}@localhost:5432" }
  let(:admin_database_url) { "#{database_url_without_db}/postgres" }
  let(:database_url) { "#{database_url_without_db}/#{db_name}" }

  after do
    unless skip_setting_up_database
      setup_connection.close
      crud_driver.connection_pool.clear(&:close)
    end

    Foobara.reset_alls
  end

  before do
    stub_env_var("DATABASE_URL", database_url)

    unless skip_setting_up_database
      database = described_class.escape_identifier(db_name)
      setup_connection.exec("DROP DATABASE IF EXISTS #{database}")
      setup_connection.exec("CREATE DATABASE #{database}")
      database_connection = PG.connect(database_url)

      database_connection.exec("CREATE TABLE some_entity (
        id SERIAL PRIMARY KEY,
        foo INTEGER,
        bar TEXT,
        created_at TIMESTAMP NULL
      );")
      database_connection.exec("CREATE TABLE some_entity_string_id (
        id TEXT NOT NULL PRIMARY KEY,
        foo INTEGER,
        bar INTEGER
      );")
      database_connection.exec("CREATE TABLE some_other_entity (
        id SERIAL PRIMARY KEY,
        foo INTEGER NOT NULL
      );")
      database_connection.exec("CREATE TABLE some_aggregate (
        id SERIAL PRIMARY KEY,
        foo INTEGER,
        some_model JSONB NOT NULL,
        some_entities integer[]
      );")
      database_connection.exec("CREATE TABLE item (
        id SERIAL PRIMARY KEY,
        details JSONB NOT NULL
      );")
      database_connection.exec("CREATE TABLE yet_another_entity (
        pk SERIAL PRIMARY KEY,
        foo INTEGER,
        bar TEXT,
        stuff JSONB
      );")
      stub_class "Capybara", Foobara::Entity do
        attributes do
          id :integer
          name :string, :required
          age :integer, :required
          date_stuff do
            birthdays [:date]
            created_at :datetime
          end
        end
      end
      database_connection.exec("CREATE TABLE capybara (
        id SERIAL PRIMARY KEY,
        name TEXT NOT NULL,
        age INTEGER NOT NULL,
        date_stuff JSONB NOT NULL
      );")

      database_connection.close

      Foobara::Persistence.default_crud_driver = crud_driver
    end
  end

  describe "#initialize" do
    context "with no DATABASE_URL env var" do
      let(:skip_setting_up_database) { true }
      let(:creds) { nil }

      stub_env_var("DATABASE_URL", nil)

      it "raises" do
        expect {
          crud_driver
        }.to raise_error(described_class::NoDatabaseUrlError)
      end
    end

    context "with DATABASE_URL env var" do
      it "uses DATABASE_URL to connect" do
        connection = crud_driver.open_connection
        expect(connection).to be_a(PG::Connection)

        expect(connection.host).to eq("localhost")
        expect(connection.port).to eq(5432)
        expect(connection.db).to eq("foobara_test")
        expect(connection.user).to eq("testuser")
        connection.close
      end
    end

    context "with a database url" do
      it "can open transactions" do
        tx = crud_driver.open_transaction
        expect(tx.connection).to be_a(PG::Connection)
        expect(tx.connection.host).to eq("localhost")
        crud_driver.commit_transaction(tx)
      end
    end
  end
end
