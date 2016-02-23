#!/usr/lib64/fluent/ruby/bin/ruby

require 'aws-sdk-v1'
require 'json'
require 'yaml'

module AfterWriteHooks
  class SQS
    QUEUE_SUFFIX = 'redshift_etl'

    # @param [string] config_file_path
    #   The config file contains the access key, secret key, and region for the SQS queue
    #   Example: "/etc/td-agent/sqs.conf"
    # @param [string] s3_bucket
    #   Example: "change-fluentd-production"
    # @param [string] s3_path
    #   The key in S3
    #   Example: "events/petition_view/production-change_main_fluentd_hub-00/2015/05/28/21_0.json"
    # @param [string] environment
    #   Example: "development", "staging", "production"
    # @param [string] event_name
    #   Example: "petition_view", "create_petition"
    def self.run(config_file_path, s3_bucket, s3_path, environment, event_name)
      queue_config = YAML.load_file(config_file_path)
      queue_name = "#{environment}-#{QUEUE_SUFFIX}" # production-redshift_etl, staging-redshift_etl, etc.

      sqs = AWS::SQS.new(
        access_key_id: queue_config['access_key_id'],
        secret_access_key: queue_config['secret_access_key'],
        region: queue_config['region']
      )
      queue = sqs.queues.named(queue_name)
      queue.send_message({
          s3_bucket: s3_bucket,
          s3_path: s3_path,
          event_name: event_name
        }.to_json)
    end
  end
end

if ARGV.length > 1
  AfterWriteHooks::SQS.run(*ARGV)
end
