// Package ddblock provides a...
// TODO:
package ddblock

import (
	"errors"
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"

	"golang.org/x/net/context"
)

var (
	// ErrConflict is returned when trying to get a lock, but
	// someone else already has it. The caller should wait and try again.
	ErrConflict = errors.New("ddbmutex: conflict, lock held by another")
)

// default values set when creating a the Mutex.
var (
	DefaultTableName = "locks"
	DefaultTTL       = time.Minute

	nameString    = "name"
	uuidString    = "uuid"
	expiresString = "expires"
)

// Mutex creates a lock using aws dynamodb. It uses
// credential and region information from the standard sources
// such as a config file or env variables.
type Mutex struct {
	lk sync.Mutex

	ctx    context.Context
	cancel func()

	TableName string
	TTL       time.Duration

	name     string
	fullname string
	uuid     string
}

// New creates a new mutex using dynamodb as the distributed store.
// If context is canceled the lock will be released.
func New(ctx context.Context, name string) *Mutex {
	if ctx == nil {
		ctx = context.Background()
	}
	ctx, cancel := context.WithCancel(ctx)
	return &Mutex{
		ctx:    ctx,
		cancel: cancel,

		TableName: DefaultTableName,
		TTL:       DefaultTTL,

		name:     name,
		fullname: "ddblock-" + name,
		uuid:     fmt.Sprintf("%d", time.Now().UnixNano()),
	}
}

// Name returns the name of the mutex which should uniquely identify
// it on dynamodb.
func (m *Mutex) Name() string {
	return m.name
}

// Lock creates the lock item on dynamodb. The lock is renewed every TTL/2
// to make sure the lock is kept. A nil error indicates success. An error
// of ErrConflict means someone else already has the lock. Another error
// indicates an network or dynamo error.
func (m *Mutex) Lock() error {
	go func() {
		for m.ctx.Err() == nil {
			select {
			case <-time.After(m.cleanTTL() / 2):
			case <-m.ctx.Done():
				m.Unlock()
				return
			}

			m.update()
		}
	}()

	return m.create()
}

// Unlock deletes the lock from dynamodb and allows other go get it.
func (m *Mutex) Unlock() error {
	m.cancel()
	return m.delete()
}

func (m *Mutex) create() error {
	m.lk.Lock()
	defer m.lk.Unlock()

	now := time.Now()
	params := &dynamodb.PutItemInput{
		TableName: &m.TableName,
		Item: map[string]*dynamodb.AttributeValue{
			"name": {
				S: &m.fullname,
			},
			"expires": {
				N: aws.String(strconv.FormatInt(now.Add(m.cleanTTL()).UnixNano(), 10)),
			},
			"uuid": {
				S: &m.uuid,
			},
		},
		ConditionExpression: aws.String("#name <> :name OR (#name = :name AND #exp < :exp)"),
		ExpressionAttributeNames: map[string]*string{
			"#name": &nameString,
			"#exp":  &expiresString,
		},
		ExpressionAttributeValues: map[string]*dynamodb.AttributeValue{
			":name": {
				S: &m.fullname,
			},
			":exp": {
				N: aws.String(strconv.FormatInt(now.UnixNano(), 10)),
			},
		},
	}

	_, err := getSvc().PutItem(params)
	return err
}

func (m *Mutex) update() error {
	m.lk.Lock()
	defer m.lk.Unlock()

	if m.uuid == "" {
		// has already been unlocked
		return nil
	}

	now := time.Now()
	params := &dynamodb.PutItemInput{
		TableName: &m.TableName,
		Item: map[string]*dynamodb.AttributeValue{
			"name": {
				S: &m.fullname,
			},
			"expires": {
				N: aws.String(strconv.FormatInt(now.Add(m.cleanTTL()).UnixNano(), 10)),
			},
			"uuid": {
				S: &m.uuid,
			},
		},
		ConditionExpression: aws.String("#name = :name AND #uuid = :uuid"),
		ExpressionAttributeNames: map[string]*string{
			"#name": &nameString,
			"#uuid": &uuidString,
		},
		ExpressionAttributeValues: map[string]*dynamodb.AttributeValue{
			":name": {
				S: &m.fullname,
			},
			":uuid": {
				S: &m.uuid,
			},
		},
	}

	_, err := getSvc().PutItem(params)
	if err != nil {
		panic(err)
	}
	return err
}

func (m *Mutex) delete() error {
	m.lk.Lock()
	defer m.lk.Unlock()

	if m.uuid == "" {
		// has already been unlocked successfully
		return nil
	}

	params := &dynamodb.DeleteItemInput{
		TableName: &m.TableName,
		Key: map[string]*dynamodb.AttributeValue{
			"name": {
				S: &m.fullname,
			},
		},
		ConditionExpression: aws.String("#name = :name AND #uuid = :uuid"),
		ExpressionAttributeNames: map[string]*string{
			"#name": aws.String("name"),
			"#uuid": aws.String("uuid"),
		},
		ExpressionAttributeValues: map[string]*dynamodb.AttributeValue{
			":name": {
				S: &m.fullname,
			},
			":uuid": {
				S: &m.uuid,
			},
		},
	}

	_, err := getSvc().DeleteItem(params)
	if IsAquireError(err) || err == nil {
		m.uuid = ""
		return nil
	}

	return err
}

// IsAquireError checks to see if the error returned by Lock
// is the result of someone else holding the lock. If false
// and err != nil, there was some sort of config or network issue.
func IsAquireError(err error) bool {
	if e, ok := err.(awserr.Error); ok {
		return e.Code() == "ConditionalCheckFailedException"
	}

	return false
}

func (m *Mutex) cleanTTL() time.Duration {
	ttl := m.TTL
	if ttl == 0 {
		ttl = DefaultTTL
	}

	if ttl == 0 {
		panic("ttl can not be zero")
	}

	return ttl
}

var (
	svc   *dynamodb.DynamoDB
	svcLk sync.Mutex
)

// getSvc enables the initialization on first read (ie. after config has been parsed),
// kind of like a singleton class.
func getSvc() *dynamodb.DynamoDB {
	svcLk.Lock()
	defer svcLk.Unlock()

	if svc == nil {
		c := aws.NewConfig().
			WithMaxRetries(3).
			WithRegion("us-east-1")

		svc = dynamodb.New(session.New(c))
	}

	return svc
}
