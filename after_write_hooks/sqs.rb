#!/usr/bin/env ruby

require 'aws-sdk'
require 'json'
require 'yaml'

module AfterWriteHooks
  class SQS
    QUEUE_SUFFIX = 'redshift_etl'

    # @param [string] config_file_path
    #   The config file contains the list of SQS queues
    #   Example: "/etc/td-agent/sqs.conf"
    # @param [string] s3_bucket
    #   Example: "change-fluentd-production"
    # @param [string] s3_path
    #   The key in S3
    #   Example: "events/petition_view/production-change_main_fluentd_hub-00/2015/05/28/21_0.json"
    def self.run(config_file_path, s3_bucket, s3_path)
      queue_config = YAML.load_file(config_file_path)
      environment = s3_bucket.split('-').last  # demo, production, staging, etc.
      queue_name = "#{environment}-#{QUEUE_SUFFIX}" # production-redshift_etl, staging-redshift_etl, etc.
      event_name = s3_path.split('/')[1] # petition_view, share_petition, etc.

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
