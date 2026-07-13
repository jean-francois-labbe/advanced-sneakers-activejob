# frozen_string_literal: true

require 'set'

module AdvancedSneakersActiveJob
  # Bounded power-of-two TTL delayed publisher.
  #
  # `levels` is configurable via `AdvancedSneakersActiveJob.config.delayed_delivery_levels`
  # (default 20 -> max_delay ~12.1 days). Higher values cover larger delays at
  # the cost of one additional quorum queue per level. INCREASING is additive
  # and safe to roll out; DECREASING orphans messages in the now-removed level
  # queues — treat as a one-way ramp in production.
  #
  # Implementation note: parent's publish lifecycle is inherited as-is.
  # We do NOT override BunnyPublisher::Base#publish. Instead we override
  # two hooks the parent already exposes:
  #
  #   #exchange         - parent reads this once per publish to get the
  #                       target exchange. We compute the right level
  #                       exchange (or the direct delivery exchange for
  #                       delay <= 0) from the current @message_options,
  #                       which the parent sets inside its own mutex
  #                       before calling us.
  #
  #   #reset_exchange!  - parent calls this from with_errors_handling when
  #                       a Bunny::ChannelAlreadyClosed forces a channel
  #                       rebuild. We invalidate our memoized level
  #                       exchange handles so the retry hits the new
  #                       channel.
  #
  # The only thing we add at the top of #publish is a pre-step: validate
  # the delay against max_delay (raises before mutex), and rewrite the
  # caller's routing_key into the (levels + 1)-segment binary-decomposed
  # form the level topic exchanges expect. Then we hand off to super,
  # which runs the parent's full publish flow (mutex, ensure_connection,
  # with_errors_handling, callbacks, exchange.publish).
  #
  # This inherits, for free:
  #
  #   * Bunny-channel thread-safety (@mutex.synchronize wraps everything).
  #   * Lazy connection + channel open (ensure_connection!).
  #   * Connection recovery and channel rebuild on transient broker errors
  #     (with_errors_handling retries on Bunny::ChannelAlreadyClosed,
  #     Bunny::ConnectionClosedError, Bunny::NetworkFailure,
  #     Bunny::ConnectionLevelException, Timeout::Error).
  #   * Per-publish callbacks (run_callbacks(:publish)).
  class LeveledDelayedPublisher < ::BunnyPublisher::Base
    DEFAULT_LEVELS = 20

    # Maximum supported levels. AMQP topic exchange routing keys have a
    # 255-byte limit. At ~2 bytes per segment plus the destination tail,
    # this is well under that bound (60 segments + destination ~120 bytes).
    MAX_LEVELS = 60

    # Every worker queue binds to this with pattern "#.<queue_name>".
    DELIVERY_EXCHANGE = 'delay.delivery.x'

    # Safety net for messages reaching DELIVERY_EXCHANGE with no matching
    # binding (the final hop is a broker-internal dead-letter republish, so
    # `mandatory` cannot catch them). Attached to DELIVERY_EXCHANGE as its
    # alternate-exchange via POLICY, never via a declare-time argument:
    # the exchange already exists in production without that argument, and
    # redeclaring with new args raises 406 PRECONDITION_FAILED on every boot.
    UNROUTED_EXCHANGE = 'delay.delivery.unrouted.x'
    PARKING_QUEUE = 'delay.delivery.parking'

    # The level queues are quorum queues declared with x-message-ttl, which
    # RabbitMQ supports on quorum queues only from 3.10 onwards. On older
    # brokers the declare fails with a cryptic "PRECONDITION_FAILED - invalid
    # arg 'x-message-ttl'"; we fail fast with a clear message instead.
    MIN_RABBITMQ_VERSION = '3.10'

    delegate :logger, to: :'::ActiveJob::Base'

    attr_reader :dlx_exchange_name, :levels, :max_delay

    def initialize(exchange:, levels: AdvancedSneakersActiveJob.config.delayed_delivery_levels, **options)
      validate_levels!(levels)

      @levels = levels
      @max_delay = (1 << levels) - 1

      # Per-process memo of destinations already bound to DELIVERY_EXCHANGE, so
      # steady-state delayed publishes cost zero broker round-trips. Guarded by
      # its own mutex (independent of the parent's publish mutex) because the
      # JIT bind happens on a dedicated channel outside the parent's lock.
      @bound_destinations = Set.new
      @bound_destinations_mutex = Mutex.new

      # Base needs an exchange; we route per-publish so the parent's
      # @exchange is effectively a placeholder. Our #exchange override
      # picks the real per-message target.
      super(**options.merge(
        exchange: DELIVERY_EXCHANGE,
        exchange_options: { type: 'topic', durable: true }
      ))
      @dlx_exchange_name = exchange
    end

    # Declare the level topology. Idempotent. Call at boot before any worker
    # queue binds to DELIVERY_EXCHANGE.
    #
    # Accepts an optional channel override. At boot time the publisher's own
    # channel may not be open yet (BunnyPublisher::Base opens lazily on first
    # publish), so host apps should pass in an already-open channel from the
    # same connection used to declare their other queues.
    def declare_topology!(channel_override = nil)
      ch = channel_override || channel
      raise 'LeveledDelayedPublisher#declare_topology! requires an open channel' if ch.nil?

      ensure_broker_supports_quorum_ttl!(ch)

      ch.topic(DELIVERY_EXCHANGE, durable: true)

      unrouted_exchange = ch.fanout(UNROUTED_EXCHANGE, durable: true)
      ch.queue(PARKING_QUEUE, durable: true, arguments: { 'x-queue-type' => 'quorum' }).bind(unrouted_exchange)

      (0...@levels).each do |n|
        level_exchange = ch.topic(level_exchange_name(n), durable: true)
        next_dlx_name  = n.zero? ? DELIVERY_EXCHANGE : level_exchange_name(n - 1)

        level_queue = ch.queue(
          level_queue_name(n),
          durable: true,
          arguments: {
            'x-queue-type'           => 'quorum',
            'x-message-ttl'          => (1 << n) * 1000,
            'x-dead-letter-exchange' => next_dlx_name
          }
        )

        # Bit N = 1: land in this level's queue.
        level_queue.bind(level_exchange, routing_key: bit_pattern(n, '1'))

        # Bit N = 0: forward straight to next-lower exchange.
        next_exchange = ch.topic(next_dlx_name, durable: true)
        next_exchange.bind(level_exchange, routing_key: bit_pattern(n, '0'))
      end

      logger.info { "LeveledDelayedPublisher: topology declared (#{@levels} levels, max #{@max_delay}s)" } if defined?(::Rails)

      nil
    end

    # Adapter calls this with routing_key=<destination> and headers={'delay'=>N}.
    # We only do pre-publish work here (validation + routing-key rewrite),
    # then hand off to the parent's full publish lifecycle via super.
    def publish(message, options = {})
      delay = options.dig(:headers, 'delay').to_i

      if delay > @max_delay
        raise DelayTooLargeError,
              "delay #{delay}s exceeds max #{@max_delay}s (~#{@max_delay / 86_400} days)"
      end

      if delay > 0
        destination = options[:routing_key].to_s

        # JIT bind BEFORE handing off to super. The parent's #publish runs
        # inside a non-reentrant mutex and its with_errors_handling does NOT
        # retry Bunny::NotFound, so the bind must happen outside that locked
        # flow — on its own dedicated channel (see #ensure_destination_binding!).
        ensure_destination_binding!(destination)

        options = options.merge(routing_key: build_routing_key(delay, destination))

        logger.debug do
          "LeveledDelayedPublisher: publishing to [#{level_exchange_name(highest_set_bit(delay))}] " \
          "with routing_key [#{options[:routing_key]}] and delay [#{delay}]"
        end
      end

      super(message, options)
    end

    # OVERRIDE: parent reads `exchange` once per publish to get the target.
    # We pick the right one based on the current message's delay header.
    #
    # @message_options is set by the parent inside its own @mutex.synchronize
    # block before this is called, so we can read it without additional
    # synchronization. For delay > 0 we return the level topic exchange
    # matching the highest set bit; for delay <= 0 we return a direct
    # exchange to the configured dlx_exchange_name for immediate delivery.
    def exchange
      delay = @message_options&.dig(:headers, 'delay').to_i

      if delay <= 0
        @immediate_exchange ||= channel.direct(dlx_exchange_name, durable: true)
      else
        level = highest_set_bit(delay)
        level_exchanges[level] ||= channel.topic(level_exchange_name(level), durable: true)
      end
    end

    # OVERRIDE: parent calls this from with_errors_handling when a
    # Bunny::ChannelAlreadyClosed forces a channel rebuild. The parent
    # rebuilds @channel and its own @exchange; we invalidate our memoized
    # per-level and immediate-direct handles so the retry hits the new
    # channel rather than the dead one.
    def reset_exchange!
      super
      @level_exchanges = nil
      @immediate_exchange = nil

      # A channel/connection rebuild means our per-process bind memo can no
      # longer be trusted to reflect broker state we observed on the old
      # channel; drop it so the next delayed publish re-verifies the binding.
      @bound_destinations_mutex&.synchronize { @bound_destinations&.clear }
    end

    # (levels + 1)-segment routing key: b{levels-1}...b00.<destination>
    def build_routing_key(delay, destination)
      bits = (@levels - 1).downto(0).map { |bit| (delay >> bit) & 1 }
      (bits + [destination]).join('.')
    end

    def level_queue_name(n)
      format('delay.level.%02d', n)
    end

    def level_exchange_name(n)
      format('delay.level.%02d.x', n)
    end

    private

    # Fail fast (before declaring anything) when the broker predates quorum-queue
    # message TTL support. Best-effort: if the version can't be read, proceed and
    # let the declare surface any real error rather than block a valid broker.
    def ensure_broker_supports_quorum_ttl!(ch)
      version = broker_version(ch)
      return if version.nil?

      major, minor = version.split('.', 3).first(2).map(&:to_i)
      return if major > 3 || (major == 3 && minor >= 10)

      raise BrokerVersionError,
            "LeveledDelayedPublisher requires RabbitMQ >= #{MIN_RABBITMQ_VERSION} for quorum-queue " \
            "message TTL (x-message-ttl on delay.level.* queues); broker reports version #{version.inspect}. " \
            'Upgrade the broker, or use legacy delayed delivery (config.delayed_delivery = :legacy).'
    end

    def broker_version(ch)
      version = ch.connection.server_properties['version'].to_s
      version.empty? ? nil : version
    rescue StandardError
      nil
    end

    def validate_levels!(value)
      unless value.is_a?(Integer) && value >= 1
        raise ArgumentError, "LeveledDelayedPublisher levels must be a positive Integer (got #{value.inspect})"
      end

      if value > MAX_LEVELS
        raise ArgumentError,
              "LeveledDelayedPublisher levels=#{value} exceeds MAX_LEVELS=#{MAX_LEVELS} " \
              '(AMQP routing key segment budget would be at risk)'
      end
    end

    def level_exchanges
      @level_exchanges ||= Array.new(@levels)
    end

    # Topic pattern with slot N pinned, other bit slots wild, '#' for destination.
    # Slot for bit N is at position levels-1-N (routing key is MSB-first).
    def bit_pattern(n, value)
      segments = Array.new(@levels, '*')
      segments[@levels - 1 - n] = value
      "#{segments.join('.')}.#"
    end

    # Integer log2 floor without Math.log2's float edge cases.
    def highest_set_bit(n)
      bit = -1
      remaining = n
      while remaining.positive?
        bit += 1
        remaining >>= 1
      end
      bit
    end

    # Just-in-time bind of `delay.delivery.x -> <destination>` with routing key
    # `#.<destination>`. NServiceBus does the equivalent on every delayed send
    # (ConfirmsAwareChannel#SendMessage -> BindToDelayInfrastructure). This
    # closes the window where a delayed job is published before the destination
    # queue's worker has ever booted (and thus bound itself) with the new gem.
    #
    # Contract (all deliberate, see BEP-9890):
    #   * Bind ONLY, never declare. queue_bind binds by name and needs no
    #     knowledge of the queue's declaration arguments; declaring risks
    #     406/PRECONDITION_FAILED against host-managed quorum/SLA/rate-limited
    #     queue args.
    #   * Dedicated channel, NOT the shared publish channel. A missing queue
    #     makes the bind fail 404 (Bunny::NotFound, a channel-level exception
    #     that closes its channel). Isolating it keeps that failure from
    #     churning healthy publishes on the shared channel.
    #   * Run BEFORE super, outside the parent's non-reentrant publish mutex:
    #     Bunny::NotFound is not in bunny-publisher's RETRIABLE_ERRORS and
    #     would otherwise escape the locked flow uncleanly.
    #   * 404 -> warn and continue. The broker-side parking/retention nets make
    #     the message recoverable once queue+binding appear; failing the enqueue
    #     would be a regression vs legacy behavior.
    #   * Memoized per destination per process, so steady state costs zero
    #     round-trips. Accepted staleness: if ops deletes+recreates a queue, a
    #     process that already warmed the memo will NOT re-bind until restart
    #     (or a channel reset invalidates the memo). The parking/retention net
    #     covers that gap.
    def ensure_destination_binding!(destination)
      return unless AdvancedSneakersActiveJob.config.leveled_ensure_binding_on_publish
      return if destination.empty?
      return if destination_bound?(destination)

      ensure_connection!

      bind_channel = connection.create_channel
      begin
        bind_channel.queue_bind(destination, DELIVERY_EXCHANGE, routing_key: "#.#{destination}")
        mark_destination_bound(destination)
      rescue Bunny::NotFound
        logger.warn do
          "LeveledDelayedPublisher: destination queue [#{destination}] does not exist yet; " \
          "publishing delayed message anyway (parking/retention net will recover it once the " \
          "queue and its #.#{destination} binding appear)"
        end
      ensure
        bind_channel.close if bind_channel.open?
      end
    # A broker/network error here (connection blip, closed channel, timeout)
    # must not fail the enqueue: the parent publisher's retry machinery only
    # engages inside super, and legacy behavior never raised for binding
    # problems. Publish anyway; the failed bind is not memoized, so the next
    # publish retries it, and the parking/retention nets cover the gap.
    rescue StandardError => error
      logger.warn do
        "LeveledDelayedPublisher: could not ensure destination binding for [#{destination}] " \
        "(#{error.class}: #{error.message}); publishing anyway (parking/retention net will " \
        'recover the message once the binding appears)'
      end
    end

    def destination_bound?(destination)
      bound_destinations_mutex.synchronize { bound_destinations.include?(destination) }
    end

    def mark_destination_bound(destination)
      bound_destinations_mutex.synchronize { bound_destinations.add(destination) }
    end

    def bound_destinations
      @bound_destinations ||= Set.new
    end

    def bound_destinations_mutex
      @bound_destinations_mutex ||= Mutex.new
    end
  end
end
