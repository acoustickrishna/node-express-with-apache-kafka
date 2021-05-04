var express = require('express');
var router = express.Router();

/* GET home page. */
router.get('/consumer', function (req, res, next) {
  res.render('consumerView', { title: 'Express Consumer' });
});

router.get('/provider', function (req, res, next) {
  res.render('providerView', { title: 'Express Provider' });
});

router.post('/submitproducer', function (req, res, next) {
  let defaultTopicName = req.defaultTopicName
  let message = req.body.producermsz
  let topicName = req.body.topicName || defaultTopicName
  let producer = req.producer;
  let consumer = req.consumer;

  const buffer = new Buffer.from(JSON.stringify(message));
  let payloads = [{ topic: topicName, messages: buffer }]

  producer.send(payloads, function (err, data) {
    if (err) {
      res.status(500).send(JSON.stringify({
        "messages": "Error while sending messages.",
      }));
    } else {
      res.status(201).send(JSON.stringify({
        "messages": "Message sent."
      }));
    }
  });
  setTimeout(function () {
    res.redirect('/provider');
  }, 2000)
});

router.post('/topics', function (req, res, next) {
  let defaultTopicName = req.defaultTopicName
  let topicName = req.body.topicName || defaultTopicName;
  var topics = [{
    topic: topicName,
    partitions: 2,
    replicationFactor: 3
  }];

  let client = req.client;
  client.createTopics(topics, (error, result) => {
    console.log(error, result)
    if (!error) {
      res.status(201).send(JSON.stringify({
        "messages": "Topic created."
      }));
    } else {
      res.status(500).send(JSON.stringify({
        "messages": "Error while creating topics.",
      }));
    }
  });
});

module.exports = router;
