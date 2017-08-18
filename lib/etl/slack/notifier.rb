require 'slack-notifier'

module ETL::Slack
  class Notifier 
    attr_accessor :attachments
    def self.create_instance(id)
      notifier ||= begin
        if ETL.config.core[:slack]
          slack_config = ETL.config.core[:slack]
          if slack_config[:url] && slack_config[:channel] && id
            ::ETL::Slack::Notifier.new(slack_config[:url], slack_config[:channel], id)
          end
        end
      end
      notifier
    end

    def initialize(webhook_url, channel, username)
      @notifier = Slack::Notifier.new(webhook_url, channel: channel, username: username)
      @attachments = []
    end

    def notify(message, icon_emoji: ":beetle:", attachments: @attachments)
      @notifier.ping message, icon_emoji: icon_emoji, attachments: attachments
    end
    
    def set_color(color)
      if @attachments.empty?
        @attachments = [{ "color": color }] 
      else
        @attachments[0][:color] = color 
      end
    end

    def add_text_to_attachments(txt) 
      if @attachments.empty?
        @attachments = [{ "text": txt }] 
      else
        if @attachments[0].include? :text
          @attachments[0][:text] += "\n" + txt
        else
          @attachments[0][:text] = txt
        end
      end
    end

    def add_field_to_attachments(field)
      if @attachments.empty?
        @attachments = [{ "fields": [ field ] }] 
      else
        if @attachments[0].include? :fields
          @attachments[0][:fields].push(field)
        else
          @attachments[0][:fields] = [field]
        end
      end
    end
  end
end
