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
      attributes do
        id :integer
        foo :integer
        bar :symbol
        created_at :datetime, default: -> { Time.now }
      end
      primary_key :id
    end
  end
  let(:skip_setting_up_database) { false }
  let(:credentials) { nil }
  let(:database_url) do
    "postgres://#{db_user}:#{db_password}@localhost:5432/#{db_name}"
  end

  def table
    entity_class.current_transaction_table.entity_attributes_crud_driver_table
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

      after { crud_driver.raw_connection.close }

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
        entity_class.transaction { entity_class.count }
      }.from(0).to(1)
    end
  end

  describe "#find" do
    it "can find a record" do
      created_record = entity_class.transaction do
        entity_class.create(foo: 1, bar: :foo)
      end

      record_id = created_record.id

      entity_class.transaction do |tx|
        attributes = tx.table_for(entity_class).entity_attributes_crud_driver_table.find(record_id)
        expect(attributes["foo"]).to eq("1")
        record = entity_class.load(record_id)

        expect(record).to be_a(entity_class)
        expect(record.id).to eq(record_id)
        expect(record.foo).to eq(1)
        expect(record.bar).to eq(:foo)
        expect(record.created_at).to be_a(Time)
      end
    end
  end

  describe "#update" do
    it "can update a record" do
      created_record = entity_class.transaction do
        entity_class.create(foo: 1, bar: :foo)
      end

      record_id = created_record.id

      entity_class.transaction do
        record = entity_class.load(record_id)
        record.foo = 2
      end

      record = entity_class.transaction do
        entity_class.load(record_id)
      end

      expect(record.foo).to eq(2)
    end
  end

  describe "#hard_delete" do
    it "can delete a record" do
      created_record = entity_class.transaction do
        entity_class.create(foo: 1, bar: :foo)
        entity_class.create(foo: 2, bar: :baz)
      end

      record_id = created_record.id

      expect {
        entity_class.transaction do
          record = entity_class.load(record_id)
          record.hard_delete!
        end
      }.to change {
        entity_class.transaction { entity_class.count }
      }.from(2).to(1)
    end
  end

  describe "#hard_delete_all" do
    it "deletes all records" do
      entity_class.transaction do
        entity_class.create(foo: 1, bar: :foo)
        entity_class.create(foo: 2, bar: :baz)
      end

      expect {
        entity_class.transaction do
          table.hard_delete_all
        end
      }.to change {
        entity_class.transaction { entity_class.count }
      }.from(2).to(0)
    end
  end

  describe "#all" do
    it "yields all records" do
      entity_class.transaction do
        111.times do
          entity_class.create(foo: 1, bar: :foo)
        end
      end

      entity_class.transaction do
        expect(table.all.to_a.size).to eq(111)
        expect(table.all(page_size: 10).to_a.size).to eq(111)
        expect(entity_class.all.first.foo).to eq(1)
      end
    end
  end
end
