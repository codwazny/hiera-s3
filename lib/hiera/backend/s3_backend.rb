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
                if defined? Config[:s3][:key]
                    s3 = AWS::S3.new(
                      :access_key_id     => Config[:s3][:key],
                      :secret_access_key => Config[:s3][:secret])
                else
                    # If credentials not defined, try IAM roles
                    s3 = AWS::S3.new
                end
                options = {}
                if defined? Config[:s3][:encryption_key_path]
                    options[:encryption_key] = IO.read(Config[:s3][:encryption_key_path]).strip
                end
                answer = nil
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
