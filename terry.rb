# encoding : utf-8
require "rubygems"
require "bundler/setup"

# get all the gems in
Bundler.require(:default)
require "digest/sha1"
require "yaml"
require "fileutils"
require 'syslog'

def is_mac?
  RUBY_PLATFORM.downcase.include?("darwin")
end

def is_linux?
   RUBY_PLATFORM.downcase.include?("linux")
end

def environment
  return "development" if is_mac?
  return "production"
end


class SimpleLogger
  def initialize(file)
    @log_file = file
  end

  def info(msg)
    write("info",msg)
  end
  def warn(msg)
    write("warn",msg)
  end
  def error(msg)
    write("error",msg)
  end
  def write(level, msg)
    File.open(log_file, "a") { |f| f.puts "#{level[0].capitalize} :: #{Time.now.to_s} : #{msg}"}
  end
end

class Database
  attr_accessor :database, :username, :hostip, :pwd_string, :redis_queue
  attr_accessor :app_name, :connection, :config, :status
  def initialize(database, username, hostip, pwd_string, app_name)
    current_path = File.expand_path(File.dirname(__FILE__))
    @config = YAML.load_file("#{current_path}/config.yml")[environment]
    @database = database
    @username = username
    @hostip = hostip
    @app_name = app_name
    @pwd_string = pwd_string
    @redis_queue = Redis.new(:host => @config['redis']['host'],
      :port => @config['redis']['port'],
      :password => @config['redis']['password'],
      :db => @config['redis']['db'])
    # PGconn.connect( :dbname => 'test', :port => 5432 )
    # PGconn.new(host, port, options, tty, dbname, user, password) ->  PGconn
    @connection = PGconn.connect(:user => config['postgresql']['user'],
      :password => config['postgresql']['password'],
      :host => config['postgresql']['host'],
      :sslmode => "require",
      :dbname => "template1")
  end

  def create_db
    begin
      req_t = "CREATE"
      req_t = "ALTER" if db_exist?
      result = connection.exec("#{req_t} DATABASE #{database} TEMPLATE template0;")
      result = connection.exec("GRANT ALL ON DATABASE #{database} TO #{username};")
      set_status("created db", nil)
    rescue => e
      set_status("failed on db", {"message" => e.message, "backtrace" => result + "\n\n" + e.backtrace})
    end
  end

  def create_user
    begin
      password = Digest::SHA1.hexdigest(config['db_token'] + '-' + pwd_string)
      req_t = "CREATE"
      req_t = "ALTER" if user_exist?
      connection.exec("#{req_t} ROLE #{username} WITH PASSWORD '#{password}' LOGIN;")
      set_status("created user", nil)
    rescue => e
      set_status("failed on user", {"message" => e.message, "backtrace" => e.backtrace})
    end
  end
  
  def user_exist?
    list_u = "SELECT usename FROM pg_catalog.pg_user;"
    list = connection.exec(list_u)
    exist = false
    list.to_a.each { |us| exist = true if us['usename'] == username }
    return exist
  end

  def db_exist?
    list_db = "SELECT datname FROM pg_database;"
    list = connection.exec(list_db)
    exist = false
    list.to_a.each { |db| exist = true if db['datname'] == database }
    return exist
  end

  def destroy_db
    begin
      del_db = "DROP DATABASE #{database}"
      connection.exec(del_db) if db_exist?
      set_status("destroyed db", nil)
    rescue => e
      set_status("failed on db", {"message" => e.message, "backtrace" => e.backtrace})
    end
  end

  def destroy_user
    begin
      del_user = "DROP ROLE #{username}"
      connection.exec(del_user) if db_exist?
      set_status("destroyed user", nil)
    rescue => e
      set_status("failed on user", {"message" => e.message, "backtrace" => e.backtrace})
    end
  end

  def to_h
    return { "database" => database,
      "username" => username,
      "passwd_string" => pwd_string}
  end

  def set_status(status_string, error_h = nil)
    # key is app name
    # { "database" => string,
    #   "username" => string,
    #   "passwd_string" => string,    # not the real password, a secret token is used to salt before hashing
    #   "status" => status,           # status of the db "waiting", "created", "destroyed"
    #   "started_at" => datetime,     # the time when the app was added in the queue
    #   "finished_at" => datetime,    # the time when the app was properly deployed
    # "error" => {"message" => "", "backtrace" => ""},
    # }
    old_status = JSON.parse(redis_queue.get(app_name)) if redis_queue.get(app_name) != nil
    start_time = Time.now.to_s
    finish_time = Time.now.to_s
    start_time = old_status['started_at'] if old_status != nil
    arh = self.to_h
    self.status = status_string
    arh["status"] = status_string
    arh['started_at'] = start_time
    arh['finished_at'] = finish_time
    arh['error'] = error_h if error_h != nil
    redis_queue.set(app_name, arh.to_json)
  end
end

@current_path = File.expand_path(File.dirname(__FILE__))
require "#{@current_path}/lib/remote_syslog"
@config = YAML.load_file("#{@current_path}/config.yml")[environment]
@redis = Redis.new(:host => @config['redis']['host'], :port => @config['redis']['port'], :password => @config['redis']['password'], :db => @config['redis']['db'])

LOGGER = RemoteSyslog.new(@config["remote_log_host"], @config["remote_log_port"]) if environment == "production"
LOGGER = SimpleLogger.new("sinatra.log") if environment == "development"

def logger
  LOGGER
end

# in redis/2
#  {"database" => string,         # database name
#   "username" => string,         # the name of the user
#   "hostip" => string,           # the ip address of the server doing the requests
#   "token" => string,            # token string that will be slated with secret token (from config, shared with cuddy)
#   "action" => string,           # action to do (create, destroy)
#   "app" => string               # the name of the app
# }
while true
  puts "in loop" unless environment == "production"
  queue = []
  queue = JSON.parse(@redis.get("queue")) unless @redis.get("queue") == nil
  while queue.size > 0
    app = queue.pop
    logger.info("db for #{app["app"]} out of the queue")
    one_db = Database.new(app["database"], app['username'], app['hostip'], app['token'], app["app"])
    if (app["action"] == "create")
      if (one_db.db_exist? || one_db.user_exist?)
        logger.error("db #{app['database']} already exists")
        one_db.set_status("already exists")
      else
        one_db.create_user
        logger.info("user created for db #{app["database"]} #{app["app"]}")
        one_db.create_db
        logger.info("database #{app["database"]} created for #{app["app"]}")
      end
    elsif (app["action"] == "destroy")
      one_db.destroy_db
      one_db.destroy_user
      logger.info("database and user destroyed for #{app['app']}")
    end
    @redis.set(queue, queue.to_json)
  end
  sleep(10)
end
