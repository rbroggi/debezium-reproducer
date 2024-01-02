package ordering

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"path"
	"strings"
	"sync"
	"testing"
	"time"

	"cloud.google.com/go/pubsub"
	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/api/types/mount"
	"github.com/docker/go-connections/nat"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"golang.org/x/sync/errgroup"
)

func TestOrdering(t *testing.T) {
	f := initFixture(t)
	t.Cleanup(f.cleanup(t))

	t.Run("create-update-delete", func(t *testing.T) {
		t.Parallel()

		receivedEvents, cleanup := startConsumer(
			t,
			f.userSubscription,
		)
		defer cleanup()

		// create, update, delete
		originalUser := newUser()
		_, err := f.userCollection.InsertOne(context.Background(), originalUser)
		require.NoError(t, err)
		updatedUser := newUser()
		updatedUser.ID = ""
		query := bson.M{"_id": originalUser.ID}
		_, err = f.userCollection.UpdateOne(context.Background(), query, bson.M{
			"$set": updatedUser,
		})
		require.NoError(t, err)
		updatedUser.ID = originalUser.ID
		_, err = f.userCollection.DeleteOne(context.Background(), query)
		require.NoError(t, err)

		assertReceivedCreateEvent(t, receivedEvents, originalUser)
		assertReceivedUpdateEvent(t, receivedEvents, originalUser, updatedUser)
		assertReceivedDeleteEvent(t, receivedEvents, updatedUser)
	})
}

func initFixture(t *testing.T) *testFixture {
	t.Helper()
	dbName := "ordering"
	networkID := newRunID()
	network, err := testcontainers.GenericNetwork(context.Background(), testcontainers.GenericNetworkRequest{
		ProviderType: testcontainers.ProviderDocker,
		NetworkRequest: testcontainers.NetworkRequest{
			Name:           networkID,
			CheckDuplicate: true,
		},
	})
	require.NoError(t, err)

	mongoSvcName := "mongodb"
	replicasetName := "rs0"
	mongoContainerRequest := testcontainers.ContainerRequest{
		Image:        "mongo:6",
		ExposedPorts: []string{"27017/tcp"},
		Cmd:          []string{"--replSet", replicasetName},
		Networks:     []string{networkID},
		WaitingFor: wait.ForAll(
			wait.ForLog("Waiting for connections"),
			wait.ForListeningPort("27017/tcp"),
		),
		Name:       mongoSvcName,
		Privileged: false,
		LifecycleHooks: []testcontainers.ContainerLifecycleHooks{
			{
				PostStarts: []testcontainers.ContainerHook{
					initReplicaSet,
					waitForMasterElection,
				},
			},
		},
	}

	mongoContainer, err := testcontainers.GenericContainer(context.Background(), testcontainers.GenericContainerRequest{
		ContainerRequest: mongoContainerRequest,
		Started:          true,
	})
	require.NoError(t, err)

	hostPortMongo, err := mongoContainer.MappedPort(context.Background(), "27017")
	require.NoError(t, err)
	url := fmt.Sprintf("mongodb://localhost:%s/?connect=direct", hostPortMongo.Port())
	// Set client options
	clientOptions := options.Client().ApplyURI(url)
	mongoClient, err := mongo.Connect(context.Background(), clientOptions)
	require.NoError(t, err)
	require.NoError(t, mongoClient.Ping(context.Background(), nil))
	testCollectionName := "test"
	db := mongoClient.Database(dbName)
	testColl := db.Collection(testCollectionName)
	_, err = testColl.Indexes().CreateOne(context.Background(), mongo.IndexModel{
		Keys: bson.D{
			{Key: "msg_id", Value: 1},
		},
	})
	require.NoError(t, err)
	require.NoError(t, db.RunCommand(context.Background(), bson.D{
		{"collMod", testCollectionName},
		{"changeStreamPreAndPostImages", bson.M{
			"enabled": true,
		}},
	}).Err())
	userCollectionName := "user"
	userColl := db.Collection(userCollectionName)
	_, err = userColl.Indexes().CreateOne(context.Background(), mongo.IndexModel{
		Keys: bson.D{
			{Key: "msg_id", Value: 1},
		},
	})
	require.NoError(t, err)
	require.NoError(t, db.RunCommand(context.Background(), bson.D{
		{"collMod", userCollectionName},
		{"changeStreamPreAndPostImages", bson.M{
			"enabled": true,
		}},
	}).Err())

	pubsubProjectID := "test"
	pubsubSvcName := "pubsub"
	pubSubInternalPort := "8085"
	pubsubRequest := testcontainers.ContainerRequest{
		Image:        "gcr.io/google.com/cloudsdktool/google-cloud-cli:458.0.1-emulators",
		ExposedPorts: []string{fmt.Sprintf("%s/tcp", pubSubInternalPort)},
		Cmd: []string{
			"gcloud",
			"beta",
			"emulators",
			"pubsub",
			"start",
			fmt.Sprintf("--project=%s", pubsubProjectID),
			fmt.Sprintf("--host-port=0.0.0.0:%s", pubSubInternalPort),
		},
		Networks: []string{networkID},
		WaitingFor: wait.ForAll(
			wait.ForListeningPort(nat.Port(fmt.Sprintf("%s/tcp", pubSubInternalPort))),
		),
		Name: pubsubSvcName,
		Env: map[string]string{
			"PUBSUB_EMULATOR_HOST": fmt.Sprintf("0.0.0.0:%s", pubSubInternalPort),
		},
	}
	pubsubContainer, err := testcontainers.GenericContainer(context.Background(), testcontainers.GenericContainerRequest{
		ContainerRequest: pubsubRequest,
		Started:          true,
	})
	require.NoError(t, err)
	hostPortPubsub, err := pubsubContainer.MappedPort(context.Background(), nat.Port(pubSubInternalPort))
	require.NoError(t, err)
	require.NoError(t, os.Setenv("PUBSUB_EMULATOR_HOST", fmt.Sprintf("localhost:%s", hostPortPubsub.Port())))

	psClient, err := pubsub.NewClient(context.Background(), pubsubProjectID)
	require.NoError(t, err)
	topicPrefix := dbName

	testCollectionFullName, testTopicName, testSubscription := collectionTopicSubscription(topicPrefix, dbName, testCollectionName)
	userCollectionFullName, userTopicName, userSubscription := collectionTopicSubscription(topicPrefix, dbName, userCollectionName)

	require.NoError(t, createProjectStructure(context.Background(), psClient, PubsubProject{
		TopicSubscribers: []TopicSubscribers{
			{
				Topic:         testTopicName,
				Subscriptions: []string{testSubscription},
			},
			{
				Topic:         userTopicName,
				Subscriptions: []string{userSubscription},
			},
		},
	}))

	wd, err := os.Getwd()
	require.NoError(t, err)
	hostPath := path.Join(wd, "conf")
	containerPath := "/debezium/conf"
	mongoDockerConnectionStringURI := fmt.Sprintf("mongodb://%s:%s/%s?readPreference=primary&ssl=false&replicaSet=%s", mongoSvcName, "27017", dbName, replicasetName)
	debeziumRequest := testcontainers.ContainerRequest{
		Image:        "debezium/server:2.4.2.Final",
		ExposedPorts: []string{"8080/tcp", "1099/tcp"},
		Networks:     []string{networkID},
		WaitingFor: wait.ForHTTP("/q/health").WithPort("8080/tcp").WithStatusCodeMatcher(
			func(status int) bool {
				return status == http.StatusOK
			},
		),
		HostConfigModifier: func(config *container.HostConfig) {
			config.Mounts = []mount.Mount{
				{
					Type:   mount.TypeBind,
					Source: hostPath,
					Target: containerPath,
				},
			}
		},
		Name: "debezium",
		Env: map[string]string{
			"QUARKUS_LOG_LEVEL":                            "DEBUG",
			"PUBSUB_EMULATOR_HOST":                         fmt.Sprintf("%s:%s", pubsubSvcName, pubSubInternalPort),
			"DEBEZIUM_SINK_TYPE":                           "pubsub",
			"DEBEZIUM_SINK_PUBSUB_PROJECT_ID":              pubsubProjectID,
			"DEBEZIUM_SINK_PUBSUB_ADDRESS":                 "pubsub:8085",
			"DEBEZIUM_SINK_PUBSUB_ORDERING_ENABLED":        "true",
			"DEBEZIUM_FORMAT_VALUE":                        "json",
			"DEBEZIUM_FORMAT_VALUE_SCHEMAS_ENABLE":         "false",
			"DEBEZIUM_FORMAT_KEY_SCHEMAS_ENABLE":           "false",
			"DEBEZIUM_SOURCE_CONNECTOR_CLASS":              "io.debezium.connector.mongodb.MongoDbConnector",
			"DEBEZIUM_SOURCE_MONGODB_CONNECTION_STRING":    mongoDockerConnectionStringURI,
			"DEBEZIUM_SOURCE_OFFSET_STORAGE_FILE_FILENAME": "data/offsets.dat",
			"DEBEZIUM_SOURCE_SNAPSHOT_MODE":                "never",
			"DEBEZIUM_SOURCE_OFFSET_FLUSH_INTERVAL_MS":     "0",
			"DEBEZIUM_SOURCE_TOPIC_PREFIX":                 topicPrefix,
			"DEBEZIUM_SOURCE_CAPTURE_MODE":                 "change_streams_update_full_with_pre_image",
			"DEBEZIUM_SOURCE_DATABASE_INCLUDE_LIST":        dbName,
			"DEBEZIUM_SOURCE_COLLECTION_INCLUDE_LIST":      strings.Join([]string{testCollectionFullName, userCollectionFullName}, ","),
			"DEBEZIUM_SOURCE_TOMBSTONES_ON_DELETE":         "false",
			"DEBEZIUM_SOURCE_POLL_INTERVAL_MS":             "100",
			"JAVA_OPTS":                                    "-javaagent:/debezium/conf/jmx_prometheus_javaagent-0.19.0.jar=1099:/debezium/conf/config.yaml ",
		},
	}
	debeziumContainer, err := testcontainers.GenericContainer(context.Background(), testcontainers.GenericContainerRequest{
		ContainerRequest: debeziumRequest,
		Started:          true,
	})
	require.NoError(t, err)

	// wiring readiness
	waitingMongoDebeziumPubsubSubscriberWiring(t, psClient, testSubscription, testColl)

	sub := psClient.Subscription(userSubscription)
	return &testFixture{
		network:           network,
		mongoClient:       mongoClient,
		dbName:            dbName,
		mongoContainer:    mongoContainer,
		pubsubContainer:   pubsubContainer,
		debeziumContainer: debeziumContainer,
		userCollection:    userColl,
		userSubscription:  sub,
	}
}

type testFixture struct {
	mongoClient       *mongo.Client
	dbName            string
	mongoContainer    testcontainers.Container
	pubsubContainer   testcontainers.Container
	debeziumContainer testcontainers.Container
	network           testcontainers.Network
	userCollection    *mongo.Collection
	userSubscription  *pubsub.Subscription
}

func (f *testFixture) cleanup(t *testing.T) func() {
	return func() {
		timeout := 2 * time.Second
		ctx, cnl := context.WithTimeout(context.Background(), timeout)
		defer cnl()
		require.NoError(t, f.mongoClient.Disconnect(ctx))

		require.NoError(t, f.mongoContainer.Stop(context.Background(), &timeout))
		require.NoError(t, f.pubsubContainer.Stop(context.Background(), &timeout))
		require.NoError(t, f.debeziumContainer.Stop(context.Background(), &timeout))

		ctx2, cnl2 := context.WithTimeout(context.Background(), timeout)
		defer cnl2()
		require.NoError(t, f.network.Remove(ctx2))
	}
}

func initReplicaSet(ctx context.Context, container testcontainers.Container) error {
	cmd := []string{"mongosh", "--eval", "rs.initiate()"}
	status, rd, err := container.Exec(ctx, cmd)
	if err != nil {
		return err
	}

	res, err := io.ReadAll(rd)
	if err != nil {
		return fmt.Errorf("failed to read command output: %w", err)
	}

	// just display the output, so we can see what's going on.
	fmt.Println(string(res))

	if status != 0 {
		return fmt.Errorf("command %v exited with status %d", cmd, status)
	}

	return nil
}

func waitForMasterElection(ctx context.Context, container testcontainers.Container) error {
	port, err := container.MappedPort(context.Background(), "27017")
	if err != nil {
		return err
	}
	opts := options.Client()
	opts.SetDirect(true)
	opts.ApplyURI("mongodb://localhost:" + string(port))

	client, err := mongo.Connect(ctx, opts)
	if err != nil {
		return err
	}
	defer func() {
		if err := client.Disconnect(ctx); err != nil {
			log.Println("waitForMasterElection: failed to disconnect from MongoDB:", err)
		}
	}()

	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return errors.New("timeout waiting for MongoDB primary to be ready")
		case <-ticker.C:
			var result bson.M
			err := client.Database("admin").RunCommand(ctx, bson.D{primitive.E{Key: "isMaster", Value: 1}}).Decode(&result)
			if err != nil {
				return fmt.Errorf("error checking MongoDB primary status: %v", err)
			}
			if result["ismaster"] == true {
				return nil
			}
		}
	}
}

func createProjectStructure(ctx context.Context, client *pubsub.Client, pss PubsubProject) error {
	for _, topicSubs := range pss.TopicSubscribers {
		topic, err := client.CreateTopic(ctx, topicSubs.Topic)
		if err != nil {
			return fmt.Errorf("error creating topic %s: %v", topicSubs.Topic, err)
		}
		topic.EnableMessageOrdering = true
		for _, sub := range topicSubs.Subscriptions {
			if _, err := client.CreateSubscription(ctx, sub, pubsub.SubscriptionConfig{
				Topic:                 topic,
				EnableMessageOrdering: true,
			}); err != nil {
				return fmt.Errorf("error creating subscription %s: %v", topicSubs.Subscriptions, err)
			}
		}
	}
	return nil
}

// PubsubProject defines the structure of a project - which topics and which
// subscribers for each topic
type PubsubProject struct {
	TopicSubscribers []TopicSubscribers
}

type TopicSubscribers struct {
	Topic         string
	Subscriptions []string
}

func collectionTopicSubscription(topicPrefix, dbName, collectionName string) (string, string, string) {
	collectionFullName := fmt.Sprintf("%s.%s", dbName, collectionName)
	topicName := fmt.Sprintf("%s.%s", topicPrefix, collectionFullName)
	subscription := topicName + "_sub"
	return collectionFullName, topicName, subscription
}

func newRunID() string {
	const runIDSize = 7
	return uuid.New().String()[0:runIDSize]
}

func waitingMongoDebeziumPubsubSubscriberWiring(
	t *testing.T,
	pubsubClient *pubsub.Client,
	testCDCSubscription string,
	testColl *mongo.Collection,
) {
	testSubscriber := pubsubClient.Subscription(testCDCSubscription)
	done := make(chan struct{})
	ctx, cnl := context.WithCancel(context.Background())
	defer cnl()
	once := sync.Once{}
	go func() {
		require.NoError(t, testSubscriber.Receive(ctx, func(ctx context.Context, msg *pubsub.Message) {
			once.Do(func() { close(done) })
			msg.Ack()
		}))
	}()

	// keeps inserting elements into mongo collection to generate CDC events
	go func() {
		for i := 0; i < 100; i++ {
			select {
			case _, ok := <-done:
				if !ok {
					return
				}
			default:
				_, err := testColl.InsertOne(context.Background(), bson.M{"test": "test"})
				require.NoError(t, err)
				time.Sleep(1 * time.Second)
			}
		}
	}()

	select {
	case <-done:
	case <-time.After(15 * time.Second):
		t.Error("never received any test CDC event - mongo-debezium-pubsub-subscriber wiring is probably wrong")
	}
	cnl()
}

type user struct {
	ID   string            `bson:"_id,omitempty"`
	Data map[string]string `bson:"data"`
}

type event struct {
	Before *user
	After  *user
}

func startConsumer(
	t *testing.T,
	subscription *pubsub.Subscription,
) (receivedEvents <-chan *event, cleanup func()) {
	events := make(chan *event, 100)
	eg, ctx := errgroup.WithContext(context.Background())
	ctx, cnl := context.WithCancel(ctx)
	eg.Go(func() error {
		return subscription.Receive(ctx, func(ctx context.Context, msg *pubsub.Message) {
			t.Logf("cdcEventData: %s\n", string(msg.Data))
			cdcEvent, err := decodeIntoEvent(msg.Data)
			require.NoError(t, err)
			bytes, err := json.Marshal(cdcEvent)
			require.NoError(t, err)
			t.Logf("cdcEvent: %s\n", string(bytes))
			events <- cdcEvent
			msg.Ack()
		})
	})

	return events, func() {
		cnl()
		require.NoError(t, eg.Wait())
		close(events)
	}
}

func decodeIntoEvent(data []byte) (*event, error) {
	dbzMsg, err := debeziumDecode(data)
	if err != nil {
		return nil, err
	}
	beforeStr := dbzMsg.Before
	var userBefore *user
	if beforeStr != nil {
		userBefore = new(user)
		if err := bson.UnmarshalExtJSON([]byte(*beforeStr), false, userBefore); err != nil {
			return nil, err
		}
	}
	afterStr := dbzMsg.After
	var userAfter *user
	if afterStr != nil {
		userAfter = new(user)
		if err := bson.UnmarshalExtJSON([]byte(*afterStr), false, userAfter); err != nil {
			return nil, err
		}
	}
	return &event{
		Before: userBefore,
		After:  userAfter,
	}, nil
}

func debeziumDecode(data []byte) (*debeziumMessage, error) {
	debeziumMsg := new(debeziumMessage)
	if err := json.Unmarshal(data, debeziumMsg); err != nil {
		return nil, fmt.Errorf("could not decode msg data into debeziumMsg struct: %w", err)
	}
	return debeziumMsg, nil
}

type debeziumMessage struct {
	Op     string  `json:"op"`
	Source source  `json:"source"`
	Before *string `json:"before"`
	After  *string `json:"after"`
}

type source struct {
	Schema     string `json:"schema"`
	Collection string `json:"collection"`
}

func newUser() *user {
	u := new(user)
	u.ID = uuid.New().String()
	u.Data = make(map[string]string)
	for i := 0; i < 50; i++ {
		el := uuid.New().String()
		u.Data[el] = el
	}
	return u
}

const debeziumCDCTimeout = 5 * time.Second

func assertReceivedCreateEvent(
	t *testing.T,
	incomingEvents <-chan *event,
	expectedAfter *user,
) *event {
	select {
	case event := <-incomingEvents:
		require.NotNil(t, event.After)
		require.Nil(t, event.Before)
		require.Equal(t, expectedAfter, event.After)
		return event
	case <-time.After(debeziumCDCTimeout):
		t.Fatalf("timeout while assertion of received events - may indicate not enough events received")
	}
	return nil
}

func assertReceivedDeleteEvent(
	t *testing.T,
	incomingEvents <-chan *event,
	expectedBefore *user,
) *event {
	select {
	case event := <-incomingEvents:
		require.NotNil(t, event.Before)
		require.Nil(t, event.After)
		require.Equal(t, expectedBefore, event.Before)
		return event
	case <-time.After(debeziumCDCTimeout):
		t.Fatalf("timeout while assertion of received events - may indicate not enough events received")
	}
	return nil
}

func assertReceivedUpdateEvent(
	t *testing.T,
	incomingEvents <-chan *event,
	expectedBefore *user,
	expectedAfter *user,
) *event {
	select {
	case event := <-incomingEvents:
		require.NotNil(t, event.Before)
		require.NotNil(t, event.After)
		require.Equal(t, expectedBefore, event.Before)
		require.Equal(t, expectedAfter, event.After)
		return event
	case <-time.After(debeziumCDCTimeout):
		t.Fatalf("timeout while assertion of received events - may indicate not enough events received")
	}
	return nil
}
