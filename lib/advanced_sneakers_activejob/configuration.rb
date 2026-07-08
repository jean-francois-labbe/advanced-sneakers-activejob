# frozen_string_literal: true

require 'active_support/ordered_options'

module AdvancedSneakersActiveJob
  # Advanced Sneakers adapter allows to patch Sneakers with custom configuration.
  # It is useful when already have Sneakers workers running and you want to run ActiveJob Sneakers process with another options.
  class Configuration
    DEFAULT_SNEAKERS_CONFIG = {
      exchange: 'activejob',
      handler: AdvancedSneakersActiveJob::Handler
    }.freeze

    DEFAULTS = {
      handle_unrouted_messages: true, # create queue/binding and re-publish if message is unrouted
      activejob_workers_strategy: :include, # [:include, :exclude, :only]
      delay_proc: ->(timestamp) { (timestamp - Time.now.to_f).round }, # seconds
      delayed_queue_prefix: 'delayed',
      delayed_queue_options: { 'x-queue-mode' => 'lazy' },
      retry_delay_proc: ->(count) { AdvancedSneakersActiveJob::EXPONENTIAL_BACKOFF[count] }, # seconds
      log_level: :info, # debug logs are too noizy because of Bunny
      # :legacy or :leveled. Accepts a symbol or a callable returning one.
      # Adapter dispatches per-publish via this value.
      delayed_delivery: :legacy,
      # Number of TTL levels in the leveled delayed delivery topology.
      delayed_delivery_levels: 20,
      # When true (default), LeveledDelayedPublisher ensures the destination
      # queue's `#.<destination>` binding on delay.delivery.x exists (just-in-time,
      # memoized per process) before publishing a delayed message. Closes the
      # window where a delayed job is published before the destination worker has
      # ever booted with the new gem. Set to false to skip the JIT bind entirely.
      leveled_ensure_binding_on_publish: true,
      publish_connection: nil
    }.freeze

    # Stores arbitrary configuration options, similar to ActiveSupport::OrderedOptions.
    attr_reader :config

    def initialize
      @config = ActiveSupport::OrderedOptions.new
      DEFAULTS.each { |key, value| config[key] = value }
    end

    DEFAULTS.each_key do |name|
      define_method(name) { config[name] }
      define_method(:"#{name}=") { |value| config[name] = value }
    end

    def republish_connection=(_)
      ActiveSupport::Deprecation.warn('Republish connection is not used for bunny-publisher v0.2+')
    end

    def sneakers
      custom_config = DEFAULT_SNEAKERS_CONFIG.deep_merge(config.sneakers || {})

      if custom_config[:amqp].present? & custom_config[:vhost].nil?
        custom_config[:vhost] = AMQ::Settings.parse_amqp_url(custom_config[:amqp]).fetch(:vhost, '/')
      end

      Sneakers::CONFIG.to_hash.deep_merge(custom_config)
    end

    def sneakers=(custom)
      config.sneakers = custom
    end

    def publisher_config
      sneakers.merge(publish_connection: publish_connection)
    end
  end
end
