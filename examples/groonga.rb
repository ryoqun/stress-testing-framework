# The author disclaims copyright to this source code.

require 'fileutils'

require 'stress-test/stress-test'
require 'groonga'

module Kernel
  #def inspect
  #  ""
  #end
end

def time_stamp
  Time.now.strftime("%Yy%mm%dd%Hh%Mm%Ss%Nns")
end

module GroongaStressTest
  class Table < StressTest::Resource
    attr_reader :name
    attr_reader :columns
    def initialize(name)
      @name = name
      @columns = {}
      @records = []
    end

    def create
      @table = Groonga::Hash.create(:name => @name, :key_type => "ShortText")
    end

    def open
      @table = Groonga::Context.default[@name]
      @table.columns.each do |column|
        @columns[column.name.split(".").last] = column
      end
      #XXX restore records?
    end

    def define_column(name)
      column = @table.define_column(name, "ShortText")
      @columns[name] = column

      column
    end

    def define_reference_column(referenced_table)
      name = to_reference_column_name(referenced_table)
      column = @table.define_column(name, referenced_table.name)
      @columns[name] = column

      column
    end

    def change_column_value(record, column, value)
      column_name = column.name.split(".").last
      @table[record.key][column_name] = value
    end

    def to_reference_column_name(table)
      "column_#{table.name.downcase}"
    end

    def add_record(key)
      record = @table.add(key)
      @records << record

      record
    end

    def delete_record(record)
      record = @records.delete(record)
      raise "bad" if record.nil?
      @table.delete(record.key)

      record
    end

    def select
      @table.select("_key:key_#{time_stamp}")
    end

    def remove
      @table.remove
    end

    def random_record
      @records.shuffle.first
    end

    def random_column
      @columns.values.shuffle.first
    end
  end

  module TableAction
    MAXIMUM_TABLE_COUNT = 1000 # XXX State should have this.

    def check_maximum_table_count
      if @state.opened_table_count > MAXIMUM_TABLE_COUNT
        raise StressTest::Error::BadRoute
      end
    end

    def bad_route?(route, flow)
      flow.profile.is_a?(StressTest::CloseResourceProfile)
    end
  end

  class CreateTable < StressTest::Action
    include TableAction

    def arguments
      check_maximum_table_count

      ["Table#{time_stamp}"]
    end

    def create_table(name)
      @state.create_resource(Table.new(name))
    end
  end

  class CloseTable < StressTest::Action
    include TableAction

    def arguments
      table = @state.random_table
      raise StressTest::Error::BadRoute if table.nil?

      [table.name]
    end

    def close_table(name)
      @state.close_resource(Table.new(name))
    end
  end

  class OpenTable < StressTest::Action
    include TableAction

    def arguments
      check_maximum_table_count

      tables = @state.database.select do |object|
        object.name =~ /\ATable/ and object.is_a?(Groonga::Hash)
      end
      raise StressTest::Error::BadRoute if tables.empty?

      table = tables.shuffle.first
      name = table.name
      raise StressTest::Error::BadRoute if @state.opened_table?(name)

      [name] # XXX don't return groonga object, arguments should return only Ruby built-in objects because arguments can be reused across multiple Database object
    end

    def open_table(name)
      @state.open_resource(Table.new(name))
    end
  end

  class DefineColumn < StressTest::Action
    def arguments
      table = @state.random_table
      raise StressTest::Error::BadRoute if table.nil?

      [table, "column_#{time_stamp}"]
    end

    def define_column(table, name)
      table.define_column(name)
    end
  end

  class DefineReferenceColumn < StressTest::Action
    def arguments
      table = @state.random_table
      raise StressTest::Error::BadRoute if table.nil?
      referenced_table = @state.random_table
      raise StressTest::Error::BadRoute if table.nil?
      raise StressTest::Error::BadRoute if table.eql?(referenced_table)
      raise StressTest::Error::BadRoute if table.columns.include?(table.to_reference_column_name(referenced_table))

      [table, referenced_table]
    end

    def define_reference_column(table, referenced_table)
      table.define_reference_column(referenced_table)
    end
  end

  class ChangeColumnValue < StressTest::Action
    def arguments
      table = @state.random_table
      raise StressTest::Error::BadRoute if table.nil?
      record = table.random_record
      raise StressTest::Error::BadRoute if record.nil?
      column = table.random_column
      raise StressTest::Error::BadRoute if column.nil?
      #p column.class.name
      value = "aaa"

      [table, record, column, value]
    end

    def change_column_value(table, record, column, value)
      table.change_column_value(record, column, value)
    end
  end

  class AddRecord < StressTest::Action
    def arguments
      table = @state.random_table
      raise StressTest::Error::BadRoute if table.nil?

      [table, "key_#{time_stamp}"]
    end

    def add_record(table, key)
      table.add_record(key)
    end
  end

  class DeleteRecord < StressTest::Action
    def arguments
      table = @state.random_table
      raise StressTest::Error::BadRoute if table.nil?
      record = table.random_record
      raise StressTest::Error::BadRoute if record.nil?

      [table, record]
    end

    def delete_record(table, record)
      table.delete_record(record)
    end
  end

  class RemoveTable < StressTest::Action
    def arguments
      table = @state.random_table
      raise StressTest::Error::BadRoute if table.nil?

      [table]
    end

    def remove_table(table)
      @state.remove_resource(table)
    end
  end

  class Select < StressTest::Action
    def arguments
      table = @state.random_table
      raise StressTest::Error::BadRoute if table.nil?

      [table]
    end

    def select(table)
      table.select
    end
  end

  module StateInitializer
    attr_reader :database
    def on_initialize
      _database_path = database_path
      puts _database_path
      @database = Groonga::Database.create(:path => _database_path)
    end

    def database_path
      ENV["DATABASE_PATH"] || choose_database_path
    end

    def choose_database_path
      random_directory = "/tmp/groonga-steress-test-databases/#{time_stamp}/"
      FileUtils.mkdir_p(random_directory)
      database_path = random_directory + "db"
      if File.exists?(database_path)
        raise "exists!"
      end
      database_path
    end
  end

  class State < StressTest::State
    include StateInitializer

    define_action :create_table, CreateTable
    define_action :open_table, OpenTable
    define_action :close_table, CloseTable
    define_action :remove_table, RemoveTable
    define_action :define_column, DefineColumn
    define_action :define_reference_column, DefineReferenceColumn
    define_action :change_column_value, ChangeColumnValue
    define_action :add_record, AddRecord
    define_action :delete_record, DeleteRecord
    define_action :select, Select

    def random_table
      resource_set.resources.shuffle.first
    end

    def opened_table?(name)
      resource_set.resources.each do |table|
        return true if table.name == name
      end
      #pp name
      #pp resource_set.resources.collect(&:name)

      false
    end

    def opened_table_count
      resource_set.resources.size
    end
  end

  class StateGroup < StressTest::StateGroup
    def on_initialize
    end

    def on_create_state(state)
    end

    class << self
      def state_class
        State
      end
    end
  end

  class Flow < StressTest::Flow
    initial_action :create_table

    route :create_table, :to => :default, :from => :default
    route :open_table, :to => :default, :from => :default
    route :close_table, :to => :default, :from => :default
    route :remove_table, :to => :default, :from => :default
    route :define_column, :to => :default, :from => :default
    route :define_reference_column, :to => :default, :from => :default
    route :change_column_value, :to => :default, :from => :default
    route :add_record, :to => :default, :from => :default
    route :delete_record, :to => :default, :from => :default
    route :select, :to => :default, :from => :default

    class << self
      def create_model_state
        State.new
      end
    end
  end
end

=begin
Thread.new do
  loop do
    puts "GC.start"
    GC.start
    seconds = Random.new.rand(0.1..1)
    puts "sleep #{seconds}"
    sleep seconds
  end
end
=end

=begin
Thread.new do
  loop do
    puts ObjectSpace.count_objects
    sleep 3
  end
end
=end

thread_count = 1

state_group = GroongaStressTest::StateGroup.new
states = thread_count.times.collect{state_group.create_state}

states.collect do |state|
  Thread.new do
    loop do
      puts state.opened_table_count
      sleep 3
    end
  end

  Thread.new do
    runner = StressTest::Runner.new(GroongaStressTest::Flow.new,
                                    states.first,
                                    :run_count => 100000000,
                                    :sleep_second => 0)
    begin
      runner.run
    #rescue
    end
  end
end.collect(&:join)

puts "exiting from ruby...."
