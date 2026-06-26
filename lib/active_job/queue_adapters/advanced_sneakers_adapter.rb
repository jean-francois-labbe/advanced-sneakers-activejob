# frozen_string_literal: true

module ActiveJob
  module QueueAdapters
    # == Active Job advanced Sneakers adapter
    #
    # A high-performance RabbitMQ background processing framework for Ruby.
    # Sneakers is being used in production for both I/O and CPU intensive
    # workloads, and have achieved the goals of high-performance and
    # 0-maintenance, as designed.
    #
    # Read more about Sneakers {here}[https://github.com/jondot/sneakers].
    #
    # To use the advanced Sneakers adapter set the queue_adapter config to +:advanced_sneakers+.
    #
    #   Rails.application.config.active_job.queue_adapter = :advanced_sneakers
    class AdvancedSneakersAdapter
      @monitor = Monitor.new

      class << self
        def enqueue(job) #:nodoc:
          AdvancedSneakersActiveJob.publisher.publish(*publish_params(job))
        end

        def enqueue_at(job, timestamp) #:nodoc:
          delay = AdvancedSneakersActiveJob.config.delay_proc.call(timestamp).to_i

          if delay.positive?
            message, options = publish_params(job)
            options[:headers] = { 'delay' => delay.to_i } # do not use x- prefix because headers exchanges ignore such headers

            select_delayed_publisher(delay: delay, job: job).publish(message, options)
          else
            enqueue(job)
          end
        end

        # Honors config.delayed_delivery (symbol or callable returning :legacy / :leveled).
        # Unknown or non-symbolizable values fall back to :legacy with a warning.
        def select_delayed_publisher(delay: nil, job: nil)
          strategy_value = AdvancedSneakersActiveJob.config.delayed_delivery
          strategy = strategy_value.respond_to?(:call) ? strategy_value.call : strategy_value

          strategy = downgrade_strategy_on_leveled_overflow(strategy, delay: delay, job: job)

          case strategy
          when :leveled, 'leveled'
            AdvancedSneakersActiveJob.leveled_delayed_publisher
          when :legacy, 'legacy'
            AdvancedSneakersActiveJob.delayed_publisher
          else
            ::ActiveJob::Base.logger.warn { "AdvancedSneakersAdapter: unknown delayed_delivery #{strategy.inspect}, using :legacy" }
            AdvancedSneakersActiveJob.delayed_publisher
          end
        end

        private

        # Returns the original strategy unchanged unless `:leveled` was chosen
        # AND the delay exceeds the leveled publisher's max_delay; in that
        # case downgrades to `:legacy` for this one publish, logs at warn
        # level, and emits an AS::Notifications event for observability.
        def downgrade_strategy_on_leveled_overflow(strategy, delay:, job:)
          return strategy unless [:leveled, 'leveled'].include?(strategy)
          return strategy if delay.nil?

          max_delay = AdvancedSneakersActiveJob.leveled_delayed_publisher.max_delay
          return strategy if delay <= max_delay

          ::ActiveJob::Base.logger.warn do
            "AdvancedSneakersAdapter: delay=#{delay}s exceeds LeveledDelayedPublisher max_delay=#{max_delay}s; " \
              "falling back to :legacy DelayedPublisher for this publish" \
              "#{job ? " (job=#{job.class.name})" : ''}."
          end
          ActiveSupport::Notifications.instrument(
            'advanced_sneakers_activejob.leveled_overflow_to_legacy',
            delay: delay,
            max_delay: max_delay,
            job_class: job&.class&.name,
            queue: (job.respond_to?(:queue_name) ? job.queue_name : nil)
          )

          :legacy
        end

        def publish_params(job)
          @monitor.synchronize do
            [
              Sneakers::ContentType.serialize(job.serialize, AdvancedSneakersActiveJob::CONTENT_TYPE),
              build_publish_params(job).merge(content_type: AdvancedSneakersActiveJob::CONTENT_TYPE)
            ]
          end
        end

        def build_publish_params(job)
          params = merged_publish_options(job)

          unless params.key?(:routing_key)
            params[:routing_key] = job.queue_name.respond_to?(:call) ? job.queue_name.call : job.queue_name
          end

          params
        end

        def merged_publish_options(job)
          publish_options = job.class.publish_options.deep_dup || {}

          publish_options.each do |key, value|
            publish_options[key] = value.call(job) if value.respond_to?(:call)
          end

          publish_options.deep_merge!(job.publish_options) if job.publish_options.present?

          publish_options
        end
      end

      delegate :enqueue, :enqueue_at, to: :'ActiveJob::QueueAdapters::AdvancedSneakersAdapter' # compatibility with Rails 5+

      class JobWrapper #:nodoc:
        def work_with_params(msg, delivery_info, headers)
          # compatibility with :sneakers adapter
          msg = ActiveSupport::JSON.decode(msg) unless headers[:content_type] == AdvancedSneakersActiveJob::CONTENT_TYPE

          msg['delivery_info'] = delivery_info
          msg['headers'] = headers
          Base.execute msg
          ack!
        end
      end
    end
  end
end
