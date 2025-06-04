require_relative "version"

Gem::Specification.new do |spec|
  spec.name = "foobara-postgresql-crud-driver"
  spec.version = Foobara::PostgresqlCrudDriverVersion::VERSION
  spec.authors = ["Miles Georgi"]
  spec.email = ["azimux@gmail.com"]

  spec.summary = "Provides a CRUD driver for reading/writing entities with a PostgreSQL database"
  spec.homepage = "https://github.com/foobara/postgresql-crud-driver"
  spec.license = "MPL-2.0"
  spec.required_ruby_version = Foobara::PostgresqlCrudDriverVersion::MINIMUM_RUBY_VERSION

  spec.metadata["homepage_uri"] = spec.homepage
  spec.metadata["source_code_uri"] = spec.homepage
  spec.metadata["changelog_uri"] = "#{spec.homepage}/blob/main/CHANGELOG.md"

  spec.files = Dir[
    "lib/**/*",
    "src/**/*",
    "LICENSE*.txt",
    "README.md",
    "CHANGELOG.md"
  ]

  spec.add_dependency "foobara", "~> 0.0.94"
  spec.add_dependency "pg"

  spec.require_paths = ["lib"]
  spec.metadata["rubygems_mfa_required"] = "true"
end
