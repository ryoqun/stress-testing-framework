# The author disclaims copyright to this source code.

require 'pp'

module StressTest
  class Error < StandardError
    class BadRoute < Error
    end
  end

  class ResourceSet
    attr_reader :resources
    def initialize
      @resources = []
    end

    def add(resource)
      @resources << resource
    end

    def delete(resource)
      raise "bad" if (resource = @resources.delete(resource)).nil?
    end

    def empty?
      @resources.empty?
    end
  end

  class Resource
    attr_writer :state

    def initialize
    end
  end

  class Action
    def initialize(state)
      @state = state
    end

    def arguments
      raise "implement"
    end

    def block
      nil
    end

    def bad_route?(route, flow)
      false
    end
  end

  class StateGroup
    def initialize
      @states = []
      @resource_set = ResourceSet.new
      on_initialize
    end

    def create_state
      state = self.class.state_class.new(@resource_set)
      state.state_group = self
      on_create_state(state)
      @states << state
      state
    end

    def on_initialize
    end

    def on_create_state(state)
    end
  end

  class State
    attr_reader :actions
    attr_writer :state_group
    attr_reader :resource_set
    def initialize(resource_set=ResourceSet.new)
      @is_initialized_state = false
      @resource_set = resource_set
      @actions = {}
      @@actions.each do |name, action|
        @actions[name] = action.new(self)
      end
      on_initialize
    end

    def action(name)
      @actions[name]
    end

    def initialized_state?
      @is_initialized_state
    end

    def is_initialized_state
      @is_initialized_state = true
    end

    attr_accessor :current_state

    def create_resource(resource)
      raise "bad: #{resource.class}" unless resource.is_a?(Resource)
      resource.state = self
      resource.create
      @resource_set.add(resource)
      resource
    end

    def remove_resource(resource)
      raise "bad: #{resource.class}" unless resource.is_a?(Resource)
      @resource_set.delete(resource)
      resource.remove
    end

    def no_resource_opened?
      @resource_set.empty?
    end

    def do_action(action, *arguments)
      #puts "#{time_stamp}: #{action}"
      @actions[action].__send__(action, *arguments)
    end

    class << self
      def define_action(name, action)
        @@actions ||= {}
        @@actions[name] = action
      end
    end
  end

  class PseudoState < State # written-by-ruby in-memory database for stress-testing real database
  end

  class Route
    def initialize(options)
      @options = options
    end

    def transit
    end

    def to
      @options[:to]
    end

    def from
      @options[:from]
    end
  end

  class Transition
    attr_accessor :action
    attr_accessor :arguments
    attr_accessor :route
  end

  class Profile
  end

  class NormalProfile < Profile
  end

  class CloseResourceProfile < Profile
  end

  class Flow
    attr_reader :profile
    def initialize
      @profile = NormalProfile.new
    end

    def begin_termination
      @profile = CloseResourceProfile.new
    end

    def next_transition(state)
      if not state.initialized_state?
        initial_transition(state)
      else
        random_transition_from_current_state(state)
      end
    end

    def initial_transition(state)
      transition = Transition.new
      transition.action = @@initial_action
      transition.arguments = state.action(transition.action).arguments
      transition.route = @@routes[@@initial_action]
      transition
    end

    def transit_state(state, transition)
      ensure_mark_state_as_initialized(state)

      state.do_action(transition.action, *transition.arguments)
      state.current_state = transition.route.to
    end

    def ensure_mark_state_as_initialized(state)
      if not state.initialized_state?
        state.is_initialized_state
      end
    end

    private
    def random_transition_from_current_state(state)
      action, arguments, route = select_random_route(state)

      transition = Transition.new
      transition.action = action
      transition.arguments = arguments
      transition.route = route
      transition
    end

    def select_random_route(state)
      begin
        try_select_random_route(state)
      rescue Error::BadRoute
        retry
      end
    end

    def try_select_random_route(state)
      action, route = randomly_select_route(state)
      if state.action(action).bad_route?(route, self)
        raise Error::BadRoute
      end
      arguments = state.action(action).arguments

      [action, arguments, route]
    end

    def randomly_select_route(state)
      routes = @@routes.select do |action, route|
        route.from == state.current_state
      end
      routes.to_a.shuffle.first
   end

    class << self
      def initial_action(action)
        @@initial_action = action
      end

      def route(action, options)
        @@routes ||= {}
        @@routes[action] = Route.new(options)
      end
    end
  end

  class Runner
    def initialize(flow, state, options={})
      @flow = flow
      @state = state
      @options = options

      initialize_sleep_second
    end

    DEFAULT_RUN_COUNT = 10
    def run_count
      @options[:run_count] || DEFAULT_RUN_COUNT
    end

    def run
      run_count.times do
        run_once
      end

      puts "begin_termination"
      @flow.begin_termination

      until @state.no_resource_opened?
        run_once
      end
    end

    def run_once
      transition = @flow.next_transition(@state) # @flow can have ideal state/ model state/expected state, Virtual State(only cares about life time of Resources) # maybe this is bad??
      #puts transition.action
      #puts transition.inspect
      #puts @state.resources.inspect
      @flow.transit_state(@state, transition)
      sleep
    end

    private
    DEFAULT_SLEEP_SECOND = 1
    def initialize_sleep_second
      @sleep_second = @options[:sleep_second] || DEFAULT_SLEEP_SECOND
    end

    def sleep
      super(@sleep_second) unless @sleep_second.zero?
    end
  end
end
