const Bacon = require("baconjs");
const util = require("util");
const kafka = require('kafka-node')


module.exports = function(app) {
  var plugin = {};
  var unsubscribes = [];

  plugin.id = "signalk-kafka-gw";
  plugin.name = "Kafka Gateway";
  plugin.description = "Kafka Gateway";

  plugin.onStop = [];

  plugin.schema = {
    title: 'Signal K - Kafka Gateway',
    type: 'object',
    required: ['port'],
    properties: {
      remoteHost: {
        type: 'string',
        title: 'Kafka Broker Url',
        default: "localhost:2181"
      },
      interval: {
        type: 'number',
        title:
        'Minimum interval between updates for this path to be sent to the server',
        default: 1
      },
      selfOnly: {
        type: 'boolean',
        title: 'Only send data for self',
        default: true
      }
    },
  };
  
  plugin.start = function(options) {
    plugin.onStop = [];
    
    var HighLevelProducer = kafka.HighLevelProducer;
    plugin.client = new kafka.Client("localhost:2181"),
    plugin.producer = new HighLevelProducer(plugin.client);

    plugin.producer.on('ready', function () {
      app.debug("ready")
      startSending(options, plugin.producer, plugin.onStop)
    });

    plugin.producer.on('error', function(err) {
      app.error("error: " + err)
    });
  }

  plugin.stop = function() {
    plugin.onStop.forEach(f => f());
    if ( plugin.client )
      plugin.client.close()
  };

  return plugin;
  
  function startSending(options, producer, onStop) {
    var command = {
      context: options.selfOnly || typeof options.selfOnly === 'undefined' ? "vessels.self" : "*",
      subscribe: [{
        path: "*",
        period: options.interval * 1000
      }]
    }

    app.debug("subscription: " + JSON.stringify(command))
    
    app.subscriptionmanager.subscribe(command,
                                      onStop,
                                      subscription_error,
                                      got_delta);

    /*
    options.paths.forEach(pathInterval => {
      onStop.push(
        app.streambundle
          .getSelfStream(pathInterval.path)
          .debounceImmediate(pathInterval.interval * 1000)
          .onValue(value => {
            var payloads = [
              {
                topic: pathInterval.path,
                messages:
                JSON.stringify({
                  context: 'vessels.' + app.selfId,
                  timestamp: new Date(),
                  path: pathInterval.path,
                  value: value,
                })
              }]

            debug("payloads: " + payloads[0].messages)
            
            producer.send(payloads, function (err, data) {
              if ( err ) {
                debug("send error: " + err)
              }
              debug("send data: " + JSON.stringify(data))
            });
          }))
    });
    */
  }

  function got_delta(delta) {
    var payloads = [];

    delta.updates.forEach(function(update) {
      update.values.forEach(function(value) {
        payloads.push({
          topic: 'vessels.self.' + value.path,
          messages:
          JSON.stringify({
            context: 'vessels.' + app.selfId,
            timestamp: update.timestamp,
            path: value.path,
            value: value.value,
          })
        })
      })
    })
    
    //debug("payloads: " + JSON.stringify(payloads))
    
    plugin.producer.send(payloads, function (err, data) {
      if ( err ) {
        app.error("send error: " + err)
      }
      //debug("send data: " + JSON.stringify(data))
    })
  }

  function subscription_error(err)
  {
    app.error("error: " + err)
  }
};
