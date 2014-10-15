# Class S3_backend
# Description: S3 back end for Hiera.
class Hiera
    module Backend
        class S3_backend
            def initialize
                require 'rubygems'
                require 'aws-sdk'
                Hiera.debug("S3_backend initialized")
            end
            def lookup(key, scope, order_override, resolution_type)
                key = key.dup.gsub!('::','/')
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
                    begin
                        path = File.join(source, key)
                        Hiera.debug("S3_backend invoked lookup: #{path}")
                        answer = Backend.parse_answer(s3.buckets[Config[:s3][:bucket]].objects[path].read(options).strip, scope)
                    rescue
                        Hiera.debug("Match not found in source " + source)
                    end
                end
                return answer
            end
        end
    end
end
