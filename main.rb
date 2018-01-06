require "bundler"
Bundler.require(:default)

require 'securerandom'

BUCKET_NAME = "paul-test-big-bucket-source"
ROOT_FOLDER_NAMES = [
  "root_folder_1",
  "root_folder_2",
  "root_folder_3",
  "root_folder_4"
]

def create_items(s3_client, item_count)
  item_count.to_i.times do |i|
    puts "Creating Object (#{i+1}/#{item_count})" if (i % 10).zero?
    uuid = SecureRandom.uuid

    s3_client.put_object({
      body: "a",
      bucket: BUCKET_NAME,
      key: "#{ROOT_FOLDER_NAMES.sample}/#{uuid}"
    })
  end

  puts "Added #{item_count} objects"
end

def get_item_count(s3_client)
  count = 0
  objects = s3_client.list_objects(bucket: BUCKET_NAME)
  loop do
    count += objects.contents.count

    next_marker = objects.next_marker
    break unless next_marker

    puts "Fetching next batch of objects"
    objects = s3_client.list_objects(bucket: BUCKET_NAME, marker: next_marker)
  end

  puts "Count: #{count}"
end

command = ARGV.first
unless command
  puts <<~USAGE
    Usage:
      ruby main.rb create_items 1000
      ruby main.rb get_item_count
  USAGE

  exit(1)
end

args = ARGV.drop(1)

s3_client = Aws::S3::Client.new
send(command, *[s3_client, *args])
