# fr33m0nk/clj-alpakka-kafka

`clj-alpakka-kafka` is a a simple library that wraps over [**Alpakka Kafka**](https://github.com/akka/alpakka-kafka) and offers convenience methods for easy implementation in Clojure code. 
For further documentation, do refer [Alpakka Kafka official docs](https://doc.akka.io/docs/alpakka-kafka/current/home.html).

## Usage

Functions are divided in following namespaces:

- **`fr33m0nk.akka.actor`**
  - Functions for working with `Akka Actor System`
- **`fr33m0nk.akka.restart-source`**
  - Functions for working with Restart Sources
  - Used for [adding fault tolerance and resiliency](https://doc.akka.io/docs/alpakka-kafka/current/errorhandling.html)
- **`fr33m0nk.akka.stream`**
  - Functions for working with [Akka source and flow streams](https://doc.akka.io/docs/akka/current/stream/index.html)
  - Only frequently used functions are mapped right now
- **`fr33m0nk.alpakka-kafka.consumer`**
  - Functions for working with [Alpakka Kafka Consumer Source and ConsumerControl](https://doc.akka.io/docs/alpakka-kafka/current/consumer.html#consumer)
- **`fr33m0nk.alpakka-kafka.producer`**
  - Functions for working with [Alpakka Kafka Producer Sink and Flow](https://doc.akka.io/docs/alpakka-kafka/current/producer.html)
- **`fr33m0nk.alpakka-kafka.committer`**
  - Functions for working with [Alpakka Kafka Committer Sink](https://doc.akka.io/docs/alpakka-kafka/current/consumer.html#committer-sink)
- **`fr33m0nk.alpakka-kafka.transactional`**
  - Functions for building Transaction pipelines using [Transactional Source and Sink](https://doc.akka.io/docs/alpakka-kafka/current/transactions.html)

    
## License

Copyright © 2023 Prashant Sinha

Distributed under the MIT License.
