WS_TOKEN = ENV['SUPERVISOR_TOKEN']
WS_URL = "ws://supervisor/core/api/websocket"
TCP_PORT = 8080

require 'socket'
require 'json'
require 'fcntl'
require 'logger'
require 'websocket/driver'
require 'fileutils'

class AppLogger
  def self.setup(log_level: Logger::WARN)

    logger = Logger.new(STDOUT)
    logger.level = log_level

    # Extend logger to include a method for logging with caller details
    def logger.log_error(message)
      error_location = caller(1..1).first # gets the immediate caller's details
      error_message = "[#{error_location}] #{message}"
      error(error_message)
    end

    logger.formatter = proc do |severity, datetime, _progname, msg|
      "#{severity[0]}-#{datetime.strftime("%d %H:%M:%S.%L")}: #{msg}\n"
    end

    logger
  end
end

LOG = AppLogger.setup(log_level: Logger::DEBUG) unless defined?(LOG)

# Read configuration from /data/options.json
options_file = '/data/options.json'
unless File.exist?(options_file)
  LOG.error("Configuration file #{options_file} not found")
  exit 1
end

begin
  options_content = File.read(options_file)
  options = JSON.parse(options_content, symbolize_names: true)
  LOG.debug("Configuration loaded from #{options_file} #{options}")
rescue JSON::ParserError => e
  LOG.error("Failed to parse configuration file: #{e.message}")
  exit 1
end

GENERIC_ACTION_ACCESS = options[:enable_generic_call_service]
WHITELIST = (options[:client_ip_whitelist] || '').split(',')
ENABLE_TLS = options[:use_tls]

module TimeoutInterface
  def add_timeout(callback_proc, duration)
    SelectController.instance.add_timeout(callback_proc, duration)
  end

  def timeout?(callback_proc)
    SelectController.instance.timeout?(callback_proc)
  end

  def remove_timeout(callback_proc)
    SelectController.instance.remove_timeout(callback_proc)
  end
end

module SocketInterface
  def read_proc(sock)
    sock = sock.to_io if sock.respond_to?(:to_io)
    SelectController.instance.readable?(sock)
  end

  def write_proc(sock)
    sock = sock.to_io if sock.respond_to?(:to_io)
    SelectController.instance.writable?(sock)
  end

  def add_readable(readable_proc, sock)
    sock = sock.to_io if sock.respond_to?(:to_io)
    SelectController.instance.add_sock(readable_proc, sock)
  end

  def remove_readable(sock)
    sock = sock.to_io if sock.respond_to?(:to_io)
    SelectController.instance.remove_sock(sock)
  end

  def add_writable(writable_proc, sock)
    sock = sock.to_io if sock.respond_to?(:to_io)
    SelectController.instance.add_sock(writable_proc, sock, for_write: true)
  end

  def remove_writable(sock)
    sock = sock.to_io if sock.respond_to?(:to_io)
    SelectController.instance.remove_sock(sock, for_write: true)
  end
end

module HassMessageParsingMethods
  def new_data(js_data)
    return {} unless js_data['data']
    js_data['data']['new_state'] || js_data['data']
  end

  def parse_event(js_data)
    case js_data.keys
    when ['c'] then entities_changed(js_data['c'])
    when ['a'] then entities_changed(js_data['a'])
    else
      event_type = js_data['event_type']
      return [:unknown, event_type] unless event_type

      case event_type
      when 'state_changed' then parse_state(new_data(js_data))
      when 'call_service'  then parse_service(new_data(js_data))
      else
        [:unknown, event_type]
      end
    end
  end

  def entities_changed(entities, parents = [])
    entities.each do |entity, state|
      # If data is nested under '+', use that instead
      state = state['+'] if state.key?('+')

      LOG.debug([:changed, entity, state])

      attributes = state['a']
      value      = state['s']

      update_hass_data(entity, parents, 'state', value) if value
      update_with_hash(entity, attributes, parents)     if attributes
    end
  end

  def parse_service(data)
    sd = data['service_data']
    return [] unless sd && sd['entity_id']

    Array(sd['entity_id']).map do |entity|
      "type:call_service,entity:#{entity},service:#{data['service']},domain:#{data['domain']}"
    end
  end

  def included_with_filter?(primary_key)
    return true if @filter.empty? || @filter == ['all']
    @filter.include?(primary_key)
  end

  def parse_state(message, parents = [])
    eid = message['entity_id']
    return unless eid

    # Possible ID substitution
    eid = @substitute_id.key(eid) || eid

    # Record its top-level state
    update_hass_data(eid, parents, 'state', message['state'])

    atr = message['attributes']
    return unless atr

    case atr
    when Hash
      update_with_hash(eid, atr, parents + ['attributes'])
    when Array
      update_with_array(eid, atr, parents + ['attributes'])
    end
  end

  #
  # Build data up in a hash, including the chain of parents,
  # and only join it all together in `update?`.
  #
  def update_hass_data(entity_id, parents, attr_name, value)
    return unless value && included_with_filter?(attr_name)

    # Hack for brightness
    value = 3 if attr_name == 'brightness' && [1, 2].include?(value)

    data_hash = {
      entity_id:  entity_id,
      parent_keys: parents,
      attr_name:  attr_name,
      attr_value: value
    }

    update?(data_hash)
  end

  def update?(data_hash)
    # Convert parent_keys array to a comma-separated string
    joined_parents = data_hash[:parent_keys].join('_')

    # Construct final string
    output = [
      "entity_id=#{data_hash[:entity_id]}",
      "substitute_id=#{@id_substitute[data_hash[:entity_id]]}",
      "parent_keys=#{joined_parents}",
      "attr_name=#{data_hash[:attr_name]}",
      "attr_value=#{data_hash[:attr_value]}"
    ].join('&')

    to_savant(output)
  end

  def update_with_hash(parent_key, msg_hash, parents = [])
    arr = msg_hash.map do |k, v|
      if k == 'hs_color' && v.is_a?(Array) && v.length == 2
        # HA returns [hue 0-360, sat 0-100]; Savant slider expects 0-100 for both.
        hue_pct = (v[0].to_f / 3.6).round
        sat = v[1].to_f.round
        @hs_state ||= {}
        @hs_state[parent_key] = [v[0].to_f, v[1].to_f]
        update_hass_data(parent_key, parents + ['hue'], 'hue', hue_pct) if included_with_filter?('hue')
        update_hass_data(parent_key, parents + ['saturation'], 'saturation', sat) if included_with_filter?('saturation')
      elsif included_with_filter?(k)
        update_hass_data(parent_key, parents + [k], k, v)
      end
      "#{k}:#{v}"
    end

    return unless included_with_filter?('attributes')

    # Also store a merged attributes string
    merged_attrs = arr.join(',')
    update_hass_data(
      parent_key, 
      parents + ['attributes'],
      parent_key,
      merged_attrs
    )
  end

  def update_with_array(parent_key, msg_array, parents = [])
    # If first element is a Hash, we treat it differently
    return update_hashed_array(parent_key, msg_array, parents) if msg_array.first.is_a?(Hash)
    update_hass_data(parent_key, parents, parent_key, msg_array.join(','))
  end

  def update_hashed_array(parent_key, msg_array, parents = [])
    msg_array.each_with_index do |e, i|
      new_key    = "#{parent_key}_#{i}"
      next_parents = parents + [i.to_s]  # track index as part of the chain

      case e
      when Hash
        update_with_hash(new_key, e, next_parents)
      when Array
        update_with_array(new_key, e, next_parents)
      else
        update_hass_data(new_key, next_parents, i, e)
      end
    end
  end

  def parse_result(js_data)
    LOG.debug([:jsdata, js_data])
    res = js_data['result']
    return unless res

    LOG.debug([:parsing, res.length])
    return parse_state(res) unless res.is_a?(Array)

    res.each do |e|
      LOG.debug([:parsing, e.length, e.keys])
      parse_state(e)
    end
  end
end

module HassAlarmRequests
  def alarm_arm_away(entity_id, code = nil)
    send_data(
      type: :call_service, domain: :alarm_control_panel, service: :alarm_arm_away,
      target: { entity_id: entity_id },
      service_data: { code: code }
    )
  end

  def alarm_arm_home(entity_id, code = nil)
    send_data(
      type: :call_service, domain: :alarm_control_panel, service: :alarm_arm_home,
      target: { entity_id: entity_id },
      service_data: { code: code }
    )
  end

  def alarm_disarm(entity_id, code = nil)
    send_data(
      type: :call_service, domain: :alarm_control_panel, service: :alarm_disarm,
      target: { entity_id: entity_id },
      service_data: { code: code }
    )
  end
end

module HassRequests
  def call_service(domain, service, entity_id: nil, **service_data)
    unless GENERIC_ACTION_ACCESS
      LOG.error("Generic action access is disabled")
      return
    end
    payload = {
      type: :call_service,
      domain: domain,
      service: service,
      target: { entity_id: entity_id },
      service_data: service_data
    }.compact
    send_data(payload)
  end
  
  def fan_on(entity_id, speed)
    send_data(
      type: :call_service, domain: :fan, service: :turn_on,
      service_data: { speed: speed },
      target: { entity_id: entity_id }
    )
  end

  def fan_off(entity_id, _speed)
    send_data(
      type: :call_service, domain: :fan, service: :turn_off,
      target: { entity_id: entity_id }
    )
  end

  def fan_set(entity_id, speed)
    speed.to_i.zero? ? fan_off(entity_id) : fan_on(entity_id, speed)
  end

  def switch_on(entity_id)
    send_data(
      type: :call_service, domain: :light, service: :turn_on,
      target: { entity_id: entity_id }
    )
  end

  def switch_off(entity_id)
    send_data(
      type: :call_service, domain: :light, service: :turn_off,
      target: { entity_id: entity_id }
    )
  end

  def dimmer_on(entity_id, level)
    send_data(
      type: :call_service, domain: :light, service: :turn_on,
      service_data: { brightness_pct: level },
      target: { entity_id: entity_id }
    )
  end

  def dimmer_off(entity_id)
    send_data(
      type: :call_service, domain: :light, service: :turn_off,
      target: { entity_id: entity_id }
    )
  end

  def dimmer_set(entity_id, level)
    level.to_i.zero? ? dimmer_off(entity_id) : dimmer_on(entity_id, level)
  end

  def color_temp_kelvin_set(entity_id, savant_value)
    v = savant_value.to_f
    kelvin = ((v * 33) + 2200).round
    send_data(
      type: :call_service, domain: :light, service: :turn_on,
      service_data: { color_temp_kelvin: kelvin },
      target: { entity_id: entity_id }
    )
  end

    def lumaris_color_temp_kelvin_set(entity_id, savant_value)
    v = savant_value.to_f
    kelvin = ((v * 22) + 1800).round
    send_data(
      type: :call_service, domain: :light, service: :turn_on,
      service_data: { color_temp_kelvin: kelvin },
      target: { entity_id: entity_id }
    )
  end

  def hue_set(entity_id, savant_value)
    hue = (savant_value.to_f * 3.6).clamp(0.0, 360.0)
    saturation = (@hs_state[entity_id] || [0.0, 0.0])[1]
    @hs_state[entity_id] = [hue, saturation]
    send_data(
      type: :call_service, domain: :light, service: :turn_on,
      service_data: { hs_color: [hue, saturation] },
      target: { entity_id: entity_id }
    )
  end

  def saturation_set(entity_id, savant_value)
    saturation = savant_value.to_f.clamp(0.0, 100.0)
    hue = (@hs_state[entity_id] || [0.0, 0.0])[0]
    @hs_state[entity_id] = [hue, saturation]
    send_data(
      type: :call_service, domain: :light, service: :turn_on,
      service_data: { hs_color: [hue, saturation] },
      target: { entity_id: entity_id }
    )
  end

  def shade_set(entity_id, level)
    send_data(
      type: :call_service, domain: :cover, service: :set_cover_position,
      service_data: { position: level },
      target: { entity_id: entity_id }
    )
  end

  def lock_lock(entity_id)
    send_data(
      type: :call_service, domain: :lock, service: :lock,
      target: { entity_id: entity_id }
    )
  end

  def unlock_lock(entity_id)
    send_data(
      type: :call_service, domain: :lock, service: :unlock,
      target: { entity_id: entity_id }
    )
  end

  def open_garage_door(entity_id)
    send_data(
      type: :call_service, domain: :cover, service: :open_cover,
      target: { entity_id: entity_id }
    )
  end

  def button_press(entity_id)
    send_data(
      type: :call_service, domain: :button, service: :press,
      target: { entity_id: entity_id }
    )
  end

  def close_garage_door(entity_id)
    send_data(
      type: :call_service, domain: :cover, service: :close_cover,
      target: { entity_id: entity_id }
    )
  end

  def toggle_garage_door(entity_id)
    send_data(
      type: :call_service, domain: :cover, service: :toggle,
      target: { entity_id: entity_id }
    )
  end

  def outlet_on(entity_id)
    send_data(
      type: :call_service, domain: :switch, service: :turn_on,
      target: { entity_id: entity_id }
    )
  end

  def outlet_off(entity_id)
    send_data(
      type: :call_service, domain: :switch, service: :turn_off,
      target: { entity_id: entity_id }
    )
  end

  def climate_set_hvac_mode(entity_id, mode)
    send_data(
      type: :call_service, domain: :climate, service: :set_hvac_mode,
      service_data: { hvac_mode: mode },
      target: { entity_id: entity_id }
    )
  end
  
  def climate_set_single(entity_id, level)
    send_data(
      type: :call_service, domain: :climate, service: :set_temperature,
      service_data: { temperature: level },
      target: { entity_id: entity_id }
    )
  end

  def climate_set_temperature_range(entity_id, temp_low, temp_high)
    send_data(
      type: :call_service, domain: :climate, service: :set_temperature,
      service_data: { target_temp_low: temp_low, target_temp_high: temp_high },
      target: { entity_id: entity_id }
    )
  end
  
  def adb_command(entity_id, command)
  send_data(
      type: :call_service, domain: :androidtv, service: :adb_command,
      service_data: { command: command },
      target: { entity_id: entity_id }
    )
  end
 
  def remote_on(entity_id)
    send_data(
      type: :call_service, domain: :remote, service: :turn_on,
      target: { entity_id: entity_id }
    )
  end

  def remote_off(entity_id)
    send_data(
      type: :call_service, domain: :remote, service: :turn_off,
      target: { entity_id: entity_id }
    )
  end

  def remote_send_command(entity_id, command)
    send_data(
      type: :call_service, domain: :remote, service: :send_command,
      service_data: { command: command },
      target: { entity_id: entity_id }
    )
  end

  def media_player_on(entity_id)
    send_data(
      type: :call_service, domain: :media_player, service: :turn_on,
      target: { entity_id: entity_id }
    )
  end

  def media_player_off(entity_id)
    send_data(
      type: :call_service, domain: :media_player, service: :turn_off,
      target: { entity_id: entity_id }
    )
  end

  def media_player_send_command(entity_id, command)
    send_data(
      type: :call_service, domain: :media_player, service: :send_command,
      service_data: { command: command },
      target: { entity_id: entity_id }
    )
  end

  def media_player_select_source(entity_id, source)
    send_data(
      type: :call_service, domain: :media_player, service: :select_source,
      service_data: { source: source },
      target: { entity_id: entity_id }
    )
  end

  def media_player_volume_up(entity_id)
    send_data(
      type: :call_service, domain: :media_player, service: :volume_up,
      service_data: { },
      target: { entity_id: entity_id }
    )
  end

  def media_player_volume_down(entity_id)
    send_data(
      type: :call_service, domain: :media_player, service: :volume_down,
      service_data: { },
      target: { entity_id: entity_id }
    )
  end

  def media_player_set_volume(entity_id, volume)
    send_data(
      type: :call_service, domain: :media_player, service: :volume_set,
      service_data: { volume_level: (volume.to_f / 100.0) },
      target: { entity_id: entity_id }
    )
  end

  def media_player_clear_playlist(entity_id)
    send_data(
      type: :call_service, domain: :media_player, service: :clear_playlist,
      service_data: { },
      target: { entity_id: entity_id }
    )
  end

  def media_player_play_pause(entity_id)
    send_data(
      type: :call_service, domain: :media_player, service: :toggle,
      service_data: { },
      target: { entity_id: entity_id }
    )
  end

  def media_player_play(entity_id)
    send_data(
      type: :call_service, domain: :media_player, service: :media_play,
      service_data: { },
      target: { entity_id: entity_id }
    )
  end

  def media_player_pause(entity_id)
    send_data(
      type: :call_service, domain: :media_player, service: :media_pause,
      service_data: { },
      target: { entity_id: entity_id }
    )
  end

  def media_player_stop(entity_id)
    send_data(
      type: :call_service, domain: :media_player, service: :media_stop,
      service_data: { },
      target: { entity_id: entity_id }
    )
  end

  def media_player_next_track(entity_id)
    send_data(
      type: :call_service, domain: :media_player, service: :media_next_track,
      service_data: { },
      target: { entity_id: entity_id }
    )
  end

  def media_player_previous_track(entity_id)
    send_data(
      type: :call_service, domain: :media_player, service: :media_previous_track,
      service_data: { },
      target: { entity_id: entity_id }
    )
  end

  def media_player_shuffle_set(entity_id, shuffle)
    send_data(
      type: :call_service, domain: :media_player, service: :shuffle_set,
      service_data: {repeat: shuffle.downcase == 'true'},
      target: { entity_id: entity_id }
    )
  end

  def media_player_repeat_set(entity_id, repeat_mode)
    send_data(
      type: :call_service, domain: :media_player, service: :repeat_set,
      service_data: {repeat: repeat_mode},
      target: { entity_id: entity_id }
    )
  end

  def media_player_play_media(entity_id, json_params)
    params = JSON.parse(json_params)
    send_data(
      type: :call_service, domain: :media_player, service: :repeat_set,
      service_data: {repeat: repeat_mode},
      target: { entity_id: entity_id }
    )
  rescue => e
    to_savant("#{e.message}: #{json_params}")
  end

  def media_player_media_seek(entity_id, seek_position)
    params = JSON.parse(json_params)
    send_data(
      type: :call_service, domain: :media_player, service: :media_seek,
      service_data: {seek_position: seek_position},
      target: { entity_id: entity_id }
    )
  rescue => e
    to_savant("#{e.message}: #{json_params}")
  end
end


class Hass
  include TimeoutInterface
  include SocketInterface
  include HassMessageParsingMethods
  include HassRequests

  POSTFIX = "\n"

  def initialize(hass_address, token, client, filter = ['all'])
    LOG.debug([:connecting_to, hass_address])
    @address = hass_address
    @token = token
    @filter = filter
    @client = client
    @client.wait_io = true
    @ws_connected = false
    @auth_required = true
    @substitute_id = {}
    @id_substitute = {}
    @authed_queue = []
    @id = 0
    connect_websocket
    @client.on(:data, method(:from_savant))
  end

  def subscribe_entities(*entity_id)
    entity_array = entity_id.flatten
    return if entity_array.empty?

    send_json(
      type: 'subscribe_entities',
      entity_ids: entity_array
    )
  end

  def send_data(**data)
    LOG.debug(data)
    send_json(data)
  end

  def from_savant(reqs, _)
    reqs.split("\n").each do |req|
      # LOG.debug([:skipping_from_savant, req])
      cmd, *params = req.split(',')
      if cmd == 'subscribe_events' then send_json(type: 'subscribe_events')
      elsif cmd == 'subscribe_entity' then subscribe_entities(params)
      elsif cmd == 'state_filter' then @filter = params
      elsif cmd == 'substitute_ids' then substitute_ids(params)
      elsif hass_request?(cmd) then send_hass_request(cmd, *params)
      else LOG.error([:unknown_cmd, cmd, req])
      end
    end
  end

  def close_connection
    LOG.debug([:closing_hass_connection])
    stop_ping_timer
    @hass_ws.close if @hass_ws
  end

  private

  def substitute_ids(params)
    params.each_slice(2) do |k, v|
      next if v.empty?
      
      @substitute_id[k] = v
      @id_substitute[v] = k
    end
    subscribe_entities(@id_substitute.keys)
    LOG.debug([:substitute_id, @substitute_id])
  end

  def send_hass_request(cmd, *params)
    params[0] = @substitute_id[params.first] || params.first
    send(cmd, *params)
  end

  def connect_websocket
    @reconnecting = false
    ws_url = @address
    @hass_ws = NonBlockSocket::TCP::WebSocketClient.new(ws_url)
  
    @hass_ws.on :open do |_event|
      LOG.debug([:ws_connected])
      start_ping_timer
    end
  
    @hass_ws.on :message do |event|
      data_received
      handle_message(event)
    end
  
    @hass_ws.on :close do |event|
      LOG.debug([:ws_disconnected, event.code, event.reason])
      reconnect_websocket
    end
  
    @hass_ws.on :error do |event|
      LOG.error([:ws_error, event])
      reconnect_websocket
    end
  end

  def cleanup_sockets
    LOG.error([:cleaning_up_sockets, @hass_ws, @client])
    remove_writable(@hass_ws) if @hass_ws
    remove_writable(@client) if @client
    remove_readable(@hass_ws) if @hass_ws
    remove_readable(@client) if @client
    @ws_connected = false
    stop_ping_timer
    @client rescue nil
    @hass_ws rescue nil
    @hass_ws = nil
    @client = nil
    LOG.error([:sockets_closed, :cleanup_complete])
  end

  def start_ping_timer
    @ws_connected = true
    LOG.debug([:sending_ping])
    send_json(type: 'ping')
    add_timeout(method(:data_received_timeout), 2)
  end

  def data_received
    # return unless @ws

    @ws_connected = true
    remove_timeout(method(:data_received_timeout))
    add_timeout(method(:start_ping_timer), 30)
  end

  def data_received_timeout
    # return unless @ws

    LOG.error([:data_received_timeout, :reconnecting])
    @ws_connected = false
    reconnect_websocket
  end
  
  def stop_ping_timer
    LOG.debug([:ping_time_stopped])
    remove_timeout(method(:start_ping_timer))
  end

  def reconnect_websocket
    return if @reconnecting

    remove_readable(@hass_ws)
    remove_writable(@hass_ws)
    @reconnecting = true
    LOG.info([:reconnecting_websocket])
    add_timeout(method(:connect_websocket), 5)
  end

  def hass_request?(cmd)
    cmd = cmd.to_sym
    LOG.debug([cmd, HassRequests.instance_methods(false)])
    HassRequests.instance_methods(false).include?(cmd.to_sym)
  end

  def handle_message(data)
    return unless (message = JSON.parse(data))
    return LOG.error([:request_failed, message]) if message['success'] == false

    LOG.debug([:handling, @hass_ws.object_id, message])
    start_ping_timer unless @ws_connected
    handle_hash(message)
  end

  def handle_hash(message)
    case message['type']
    when 'auth_required' then send_auth
    when 'event' then parse_event(message['event'])
    when 'result' then parse_result(message)
    when 'auth_ok' then auth_ok
    when 'pong' then to_savant("#{message['id']},pong,#{Time.now}")
    end
  end

  def send_auth
    @auth_required = true
    auth_message = { type: 'auth', access_token: @token }.to_json
    LOG.debug([:sending_auth])
    @hass_ws.send(auth_message)
  end

  def send_json(hash)
    # return unless @ws
    return push_to_queue(hash) if @auth_required

    @id += 1
    hash['id'] = @id
    hash = hash.to_json
    LOG.debug([:send, @hass_ws.object_id, hash])
    @hass_ws.send(hash)
  end

  def push_to_queue(hash)
    @authed_queue ||= []
    @authed_queue << hash
    LOG.debug([:waiting_to_send, @authed_queue])
  end

  def auth_ok
    @client.wait_io = false
    @auth_required = false
    @authed_queue.each { |h| send_json(h) }
    @authed_queue = []
    LOG.info([:authorization_complete])
    add_timeout(proc { to_savant("hass_websocket_connected,#{Time.now}") } , 5)
  end

  def to_savant(*message)
    return unless message

    return cleanup_sockets unless @client
    peer = @client.peeraddr
    LOG.debug([:to_savant, peer[1,2], @hass_ws.object_id])
    ret = @client.write(map_message(message).join)
  rescue IOError => e
    LOG.error([:savant_io_error])
    cleanup_sockets
  end

  def map_message(message)
    Array(message).map do |m|
      next unless m

      [m.to_s.gsub(POSTFIX, ''), POSTFIX]
    end
  end
end

class EventBus
  @instance = nil
  class << self
    def instance
      @instance ||= new
    end

    def subscribe(...)
      instance.subscribe(...)
    end

    def unsubscribe(...)
      instance.unsubscribe(...)
    end

    def publish(...)
      instance.publish(...)
    end
  end

  private_class_method :new

  def initialize
    @subscribers = {} # Hash.new { |h, k| h[k] = [] }
  end

  def subscribe(event_path, event_name, prc)
    key = build_key(event_path, event_name)
    LOG.debug([:new_subscription, key, prc])
    @subscribers[key] ||= []
    @subscribers[key] << prc
  end

  def unsubscribe(event_path, event_name, prc)
    key = build_key(event_path, event_name)
    LOG.debug([:unsubscribing, key, prc, @subscribers[key].length])
    @subscribers[key].delete(prc)
  end

  def publish(event_path, event_name, *args)
    LOG.debug([:event_published, event_path, event_name, args])
    key = build_key(event_path, event_name)
    notify_subscribers(key, args)
  end

  private

  def build_key(path, name)
    "#{path}:#{name}"
  end

  def notify_subscribers(key, args)
    LOG.debug([:notifying, key, :count, @subscribers[key]&.length])
    @subscribers[key]&.each { |handler| handler.call(args) }
    # Notify wildcard subscribers
    wildcard_key = "#{key.split(':').first}:*"
    @subscribers[wildcard_key]&.each { |handler| handler.call(args) unless key == wildcard_key }
  end
end

module SelectHandlerMethods
  def handle_err(err_socks)
    return unless err_socks.is_a?(Array)

    LOG.debug([:error, err_socks])
    handle_readable(err_socks)
  end

  def handle_writable(writable)
    return unless writable.is_a?(Array)

    writable.each { |sock| @writable[sock]&.call }
  end

  def handle_readable(readable)
    return unless readable.is_a?(Array)

    readable.each { |sock| @readable[sock]&.call }
  end

  def handle_timeouts
    current_time = Time.now
    touts = @timeouts.keys
    touts.each do |callback_proc|
      # LOG.debug callback_proc
      timeout = @timeouts[callback_proc]
      next unless current_time >= timeout

      @timeouts.delete(callback_proc)
      callback_proc.call
    end
  end
end

class SelectController
  MAX_SOCKS = 50
  @instance = nil
  class << self
    def instance
      @instance ||= new
    end

    def run
      instance.run
    end
  end
  private_class_method :new

  include SelectHandlerMethods

  def initialize
    reset
  end

  def readable?(sock)
    @readable[sock]
  end

  def writable?(sock)
    @writable[sock]
  end

  def add_sock(call_proc, sock, for_write: false)
    raise "IO type required for socket argument: #{sock.class}" unless sock.is_a?(IO)
    raise "invalid proc detected: #{call_proc.class}" unless call_proc.respond_to?(:call)

    for_write ? @writable[sock] = call_proc : @readable[sock] = call_proc
  end

  def remove_sock(sock, for_write: false)
    # LOG.debug(["removing_#{for_write ? 'write' : 'read'}_socket", sock, sock.object_id])
    for_write ? @writable.delete(sock) : @readable.delete(sock)
  end

  def remove_readables(socks)
    socks.each { |sock| remove_sock(sock) }
    @writable.each_key { |sock| remove_sock(sock, for_write: true) }
  end

  def stop
    remove_readables(@readable.keys)
  end

  def timeout?(callback_proc)
    @timeouts[callback_proc]
  end

  def add_timeout(callback_proc, seconds)
    # LOG.debug callback_proc
    raise 'positive value required for seconds parameter' unless seconds.positive?
    raise "invalid proc detected: #{callback_proc.class}" unless callback_proc.respond_to?(:call)

    @timeouts[callback_proc] = Time.now + seconds
  end

  def remove_timeout(callback_proc)
    @timeouts.delete(callback_proc)
  end

  def reset
    @readable = {}
    @writable = {}
    @timeouts = {}
    at_exit do
      stop
    end
  end

  def run
    loop { select_socks }
    # $stdout.puts([Time.now, 'ok', Process.pid])
  rescue StandardError => e
    LOG.error([:uncaught_exception_while_select, e])
    LOG.error("Backtrace:\n\t#{e.backtrace.join("\n\t")}")
    exit
  end

  private

  def readables
    @readable.delete_if { |socket, _| socket.closed? }
    @readable.keys
  end

  def writeables
    @writable.delete_if { |socket, _| socket.closed? }
    @writable.keys
  end


  def run_select
    rd = readables
    # LOG.debug([:selecting, rd])
    raise "socks limit #{MAX_SOCKS} exceeded in select loop." if rd.length > MAX_SOCKS

    select(rd, writeables, rd, calculate_next_timeout)
  rescue IOError => e
    LOG.error([:io_error_in_select, e])
  end

  def select_socks
    # LOG.debug @readable
    readable, writable, err = run_select
    # LOG.debug readable
    return handle_err(err) if err && !err.empty?

    handle_timeouts
    handle_writable(writable) if writable
    handle_readable(readable) if readable
  end

  def calculate_next_timeout
    tnow = Time.now
    return 30 if @timeouts.empty?

    [@timeouts.values.min, tnow].max - tnow
  end
end

# SelectController.instance.setup

module NonBlockSocket; end
module NonBlockSocket::TCP; end
module NonBlockSocket::TCP::SocketExtensions; end

module NonBlockSocket::TCP::SocketExtensions::SocketIO
  CHUNK_LENGTH = 1024 * 16

  def write(data)
    return unless data

    @output_table ||= []
    @output_table << data
    return if @wait_io

    add_writable(method(:write_message), to_io)
    # LOG.debug([:added_to_write_queue, data])
  end

  private

  def read_chunk
    # LOG.debug(['reading'])
    dat = to_sock.read_nonblock(CHUNK_LENGTH)
    # LOG.debug(['read', dat])
    raise(EOFError, 'Nil return on readable') unless dat

    handle_data(dat)
  rescue EOFError, Errno::EPIPE, Errno::ECONNREFUSED, Errno::ECONNRESET => e
    LOG.debug([:read_chunk_error, :read, dat.to_s.length, e])
    on_disconnect(dat)
    on_error(e, e.backtrace)
  rescue IO::WaitReadable
    # IO not ready yet
  end

  def write_chunk
    # LOG.debug(['writing', @write_buffer])
    written = to_sock.write_nonblock(@write_buffer)
    # LOG.debug(['wrote', written])
    @write_buffer = @write_buffer[written..] || ''.dup
  rescue EOFError, Errno::EPIPE, Errno::ECONNREFUSED, Errno::ECONNRESET => e
    on_error(e, e.backtrace)
    on_disconnect
  rescue IO::WaitWritable
    # IO not ready yet
  end

  def write_message
    return next_write unless @write_buffer.empty?
    return if @output_table.empty?

    @write_buffer << @output_table.shift
    @current_output = @write_buffer
    write_chunk
    next_write
  end

  def next_write
    on_wrote(@current_output) if @write_buffer.empty?
    return unless @output_table.empty?

    on_empty
    remove_writable(to_io)
    close if @close_after_write
  end
end

module NonBlockSocket::TCP::SocketExtensions::Events
  def trigger_event(event_name, *args)
    # LOG.debug([:event, event_name, args, self])
    handler = @handlers[event_name]
    handler&.call(*args)
  end

  def on_empty
    trigger_event(:empty)
  end

  def on_error(error, backtrace)
    LOG.error([error, backtrace])
    @error_status = [error, backtrace]
    trigger_event(:error, @error_status, self)
  end

  def on_connect
    LOG.debug([:io_connected, self])
    @disconnected = false
    add_readable(method(:read_chunk), to_io)
    next_write
    trigger_event(:connect, self)
  end

  def on_disconnect(dat = nil)
    @disconnected = true
    on_data(dat) if dat
    remove_readable(to_io)
    remove_writable(to_io)
    close unless closed?
    trigger_event(:disconnect, self)
  end

  def on_data(data)
    trigger_event(:data, data, self)
  end

  def on_message(message)
    trigger_event(:message, message, self)
  end

  def on_wrote(message)
    return unless message

    @current_output&.clear
    trigger_event(:wrote, message, self)
  end
end

module NonBlockSocket::TCP::SocketExtensions
  include Events
  include SocketIO
  include SocketInterface
  include TimeoutInterface

  DEFAULT_BUFFER_LIMIT = 1024 * 16
  DEFAULT_BUFFER_TIMEOUT = 2

  attr_accessor :handlers, :read_buffer_timeout, :max_buffer_size

  def connected
    setup_buffers
    @handlers ||= {}
    on_connect
  end

  def add_handlers(handlers)
    handlers.each { |event, proc| on(event, proc) }
  end

  def on(event, proc = nil, &block)
    @handlers ||= {}
    @handlers[event] = proc || block
  end

  def to_io
    @socket
  end

  def to_sock
    @socket
  end

  def closed?
    to_sock ? to_sock.closed? : true
  end

  def close
    return if closed?

    to_sock.close
    on_disconnect unless @disconnected
  end

  private

  def setup_buffers
    @input_buffer ||= ''.dup
    @output_table ||= []
    @write_buffer ||= ''.dup
    @read_buffer_timeout ||= DEFAULT_BUFFER_TIMEOUT
    @max_buffer_size ||= DEFAULT_BUFFER_LIMIT
  end

  def handle_data(data)
    add_timeout(method(:handle_read_timeout), @read_buffer_timeout)
    LOG.debug([:handle_socket_data, object_id, data[0..10]])
    on_data(data)
    handle_message(data)
  end

  def handle_message(data)
    return unless (on_msg = @handlers[:message])
    return unless (pattern = on_msg.pattern)

    @input_buffer << data
    handle_buffer_overrun
    while (line = @input_buffer.slice!(pattern))
      on_message(line)
    end
  end

  class BufferOverrunError < StandardError; end

  def handle_buffer_overrun
    return unless @input_buffer.size > @max_buffer_size

    close
    raise BufferOverrunError, "Read buffer size exceeded for client: #{self}"
  end

  def handle_read_timeout
    return if @input_buffer.empty?

    LOG.info(["Read timeout reached for client: #{self}, clearing data from buffer: ", @input_buffer])
    @input_buffer = ''.dup
  end
end

class NonBlockSocket::TCP::Wrapper
  include NonBlockSocket::TCP::SocketExtensions

  attr_accessor :wait_io

  def initialize(socket)
    @socket = socket
    setup_buffers
  end

  def peeraddr
    @socket.peeraddr
  end
end

class NonBlockSocket::TCP::WebSocketClient
  include NonBlockSocket::TCP::SocketExtensions

  attr_reader :driver, :url

  def initialize(url, handlers: {})
    @url = url
    @uri = URI.parse(url)
    @host = @uri.host
    @port = @uri.port || 80
    @socket = nil
    @handlers = {}
    setup_buffers
    @driver = WebSocket::Driver.client(self)

    add_handlers(handlers)
    setup_websocket_handlers
    connect_nonblock
  end

  # Send a message via the WebSocket
  def send(data)
    @driver.text(data)
  end

  # Write data to the socket (called by the driver)
  def write(data)
    # LOG.debug([:sending_data, data])
    super(data)  # Ensure data is sent to the socket
  end

  def to_io
    @socket
  end

  private

  def connect_nonblock
    @socket = Socket.new(Socket::AF_INET, Socket::SOCK_STREAM, 0)
    @socket.setsockopt(Socket::SOL_SOCKET, Socket::SO_REUSEADDR, true)
    @socket.setsockopt(Socket::IPPROTO_TCP, Socket::TCP_NODELAY, 1)
    @socket.connect_nonblock(Socket.sockaddr_in(@port, @host), exception: false)
    readable?
  rescue => e
    on_error(e, e.backtrace)
    on_disconnect
  end

  def readable?
    to_io.read_nonblock(1)
    setup_io
  rescue IO::WaitWritable, IO::WaitReadable
    setup_io
  rescue Errno::ECONNREFUSED => e
    on_error(e, e.backtrace)
    on_disconnect
  end

  def setup_io
    remove_readable(to_io)
    @wait_io = false
    @driver.start  # Initiates the WebSocket handshake
    LOG.debug([:ws_driver_started])
    @handlers[:empty] = proc { add_readable(method(:read_chunk), to_io) }
    # connected
  end

  def setup_websocket_handlers
    @driver.on(:open)    { connected }
    @driver.on(:message) { |e| on_message(e.data) }
    @driver.on(:close)   { on_disconnect }
    @driver.on(:error)   { |e| on_error(e.message, e.backtrace) }
  end

  # Override handle_data to pass data to the driver
  def handle_data(data)
    # LOG.debug([:handle_ws_data, data])
    @driver.parse(data)
  end

  def connected
    super
    LOG.debug(['WebSocket connected', @host, @port])
  end
end

class NonBlockSocket::TCP::Server
  include SocketInterface
  include TimeoutInterface
  include Fcntl

  attr_reader :port

  CHUNK_LENGTH = 2048
  TCP_SERVER_FIRST_RETRY_SECONDS = 1
  TCP_SERVER_RETRY_LIMIT_SECONDS = 60
  TCP_SERVER_RETRY_MULTIPLIER = 2

  def initialize(**kwargs)
    @addr = kwargs[:host] || '0.0.0.0'
    @port = kwargs[:port] || 0
    @setup_proc = method(:setup_server)
    @handlers = kwargs[:handlers] || {}
    setup_server
  end

  def add_handlers(handlers)
    handlers.each { |event, proc| on(event, proc) }
  end

  def on(event, proc = nil, &block)
    @handlers[event] = proc || block
  end

  def setup_server
    @server ||= TCPServer.new(@addr, @port)
    @port = @server.addr[1]
    LOG.debug([self, @port])
    add_readable(method(:handle_accept), @server)
    LOG.info("Server setup complete, listening on port #{@port}")
  rescue Errno::EADDRINUSE
    port_in_use
  end

  def handle_accept
    LOG.info([:accepting_non_block_client, @port])
    client = @server.accept_nonblock
    setup_client(client)
  rescue IO::WaitReadable, IO::WaitWritable
    # If the socket isn't ready, ignore for now
  end

  def setup_client(client)
    client.fcntl(Fcntl::F_SETFL, Fcntl::O_NONBLOCK)
    client = NonBlockSocket::TCP::Wrapper.new(client)
    @handlers.each { |k, v| client.on(k, v) }
    client.connected
  end

  def close
    @server.close
  end

  def available?
    !(@server.nil? || @server.closed?)
  end

  private

  def port_in_use
    LOG.error("TCP server could not start on Port #{@port} already in use")
    @server_setup_retry_seconds ||= TCP_SERVER_FIRST_RETRY_SECONDS
    exit if @server_setup_retry_seconds > TCP_SERVER_RETRY_LIMIT_SECONDS
    add_timeout(@setup_proc, @server_setup_retry_seconds * TCP_SERVER_RETRY_MULTIPLIER) unless timeout?(@setup_proc)
  end
end


# def tcp_client_connected(client)
#   hass = Hass.new(WS_URL, WS_TOKEN, client)
# end

def tcp_client_connected(client)
  client_ip = client.peeraddr[3]
  unless WHITELIST.include?(client_ip) || WHITELIST.empty?
    LOG.error("Access denied for client: #{client_ip}")
    client.close
    return
  end
  LOG.info("Access granted for client: #{client_ip}")
  Hass.new(WS_URL, WS_TOKEN, client)
end


TCP_SERVER = NonBlockSocket::TCP::Server.new(
  port: TCP_PORT,
  handlers:{
    connect: (method(:tcp_client_connected))
  }
)

SelectController.run
