#!/usr/bin/env ruby

require 'aws-sdk-v1'
require 'json'
require 'yaml'

module AfterWriteHooks
  class SQS
    def self.run(config_file_path, s3_bucket, s3_path, sqs_prefix = '')
      event_type = s3_path.split('/')[1]
      queue_configs = self.config(config_file_path)['queues']
      default_config = queue_configs['default']
      configs = queue_configs[event_type] || default_config

      configs.each do |queue_config|
        # like "someevent-in-demo" or "someevent-in-production"
        queue_name = queue_config['name'] || "#{event_type}-in-#{s3_bucket.split('-').last}"
        queue_name = sqs_prefix + queue_name

        sqs = AWS::SQS.new(
          access_key_id: queue_config['access_key_id'],
          secret_access_key: queue_config['secret_access_key'],
          region: queue_config['region']
        )
        message = {s3_bucket: s3_bucket, s3_path: s3_path}
        queue = sqs.queues.named(queue_name)
        queue.send_message(message.to_json)
      end
    rescue AWS::SQS::Errors::NonExistentQueue => e
      STDERR.puts e.message
    end

    def self.config(config_file_path)
      @config ||= YAML.load_file(config_file_path)
    end
  end
end

if ARGV.length > 1
  AfterWriteHooks::SQS.run(*ARGV)
end
