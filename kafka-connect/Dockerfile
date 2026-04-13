FROM confluentinc/cp-kafka-connect-base:7.5.0

# FIX #3: Pre-install kafka-connect-http plugin at image build time.
# This eliminates the ~10 minute startup delay caused by downloading at runtime.
RUN confluent-hub install --no-prompt confluentinc/kafka-connect-http:1.7.3

CMD ["/etc/confluent/docker/run"]
