# Class S3_backend
# Description: S3 back end for Hiera.
class Hiera
    module Backend
        class S3_backend
            def initialize
                require 'rubygems'
                require 'aws-sdk'
                require 'yaml'
                Hiera.debug("S3_backend initialized")
            end
            def lookup(raw_key, scope, order_override, resolution_type)
                key = raw_key.gsub('::','/')
                Hiera.debug("S3_backend using lookup key: #{key}")
                if Config[:s3][:key]
                    Hiera.debug("S3_backend using AWS key: #{Config[:s3][:key]}")
                    s3 = AWS::S3.new(
                      :access_key_id     => Config[:s3][:key],
                      :secret_access_key => Config[:s3][:secret])
                else
                    Hiera.debug("S3_backend using IAM roles")
                    s3 = AWS::S3.new
                end
                options = {}
                if Config[:s3][:encryption_key_path]
                    Hiera.debug("S3_backend using encryption key from: #{Config[:s3][:encryption_key_path]}")
                    options[:encryption_key] = IO.read(Config[:s3][:encryption_key_path]).strip
                end
                answer = nil
                Hiera.debug("S3_backend using bucket: #{Config[:s3][:bucket]}")
                Backend.datasources(scope, order_override) do |source|
                    # combine the source and the key to get the path
                    path = File.join(source, key)
                    Hiera.debug("S3_backend invoked lookup: #{path}")
                    # get data from the specified path
                    bucket_data = nil
                    begin
                        bucket_data = s3.buckets[Config[:s3][:bucket]].objects[path].read(options)
                    rescue
                    end
                    # bucket_data is nil if the key is not found
                    next unless bucket_data
                    Hiera.debug("Found #{key} in #{source}")
                    Hiera.debug("Raw data: #{bucket_data}")
                    # if YAML detected, parse as YAML
                    bucket_data = YAML.load(bucket_data) if !!bucket_data[/^---[^-]/m] 
                    new_answer = Backend.parse_answer(bucket_data, scope)
                    Hiera.debug("YAML-parsed data: #{new_answer}")
                    case resolution_type
                    when :array
                        raise Exception, "Hiera type mismatch for key '#{key}': expected Array and got #{new_answer.class}" unless new_answer.kind_of? Array or new_answer.kind_of? String
                        answer ||= []
                        answer << new_answer
                    when :hash
                        raise Exception, "Hiera type mismatch for key '#{key}': expected Hash and got #{new_answer.class}" unless new_answer.kind_of? Hash
                        answer ||= {}
                        answer = Backend.merge_answer(new_answer,answer)
                    else
                        answer = new_answer
                        break
                    end
                end
                return answer
            end
        end
    end
end
