package mongomove

import (
	"context"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"github.com/AlecAivazis/survey/v2"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"golang.org/x/sync/errgroup"
)

const (
	defaultPingTimeout = 5 * time.Second
)

// Importer imports the databases from one Client to another.
type Importer struct {
	source *mongo.Client
	target *mongo.Client
}

// ImportOption is an option for the Importer.
type ImportOption func(*importConfig)

type importConfig struct {
	dbFilter    []func(string) bool
	drop        bool
	skipConfirm bool
	verbose     bool
	pingTimeout time.Duration
}

// FilterDatabases returns an ImportOption which filters databases by their
// name. If any of the provided filters returns false, the database will not be
// imported.
func FilterDatabases(filter ...func(string) bool) ImportOption {
	return func(cfg *importConfig) {
		cfg.dbFilter = append(cfg.dbFilter, filter...)
	}
}

// HasPrefix returns a function which accepts a string and returns if the string
// has the given prefix. This function can be used as a filter for the
// FilterDatabases function.
func HasPrefix(prefix string) func(string) bool {
	return func(s string) bool {
		return strings.HasPrefix(s, prefix)
	}
}

// Drop returns an ImportOption which drops existing databases in the target
// Client before importing them.
func Drop(drop bool) ImportOption {
	return func(cfg *importConfig) {
		cfg.drop = drop
	}
}

// SkipConfirm returns an ImportOption which disables the manual confirmation
// of imports. Use with caution!
func SkipConfirm(skip bool) ImportOption {
	return func(cfg *importConfig) {
		cfg.skipConfirm = skip
	}
}

// PingTimeout returns an ImportOptions which sets the timeout when pinging the
// mongo.Clients.
func PingTimeout(d time.Duration) ImportOption {
	return func(cfg *importConfig) {
		cfg.pingTimeout = d
	}
}

// Verbose returns an ImportOption which enables debugging output.
func Verbose(v bool) ImportOption {
	return func(cfg *importConfig) {
		cfg.verbose = v
	}
}

// New returns an Importer that imports databases from source to target. New
// panics if source or target is nil.
func New(source, target *mongo.Client) *Importer {
	if source == nil {
		panic("<nil> client (source)")
	}
	if target == nil {
		panic("<nil> client (target)")
	}
	return &Importer{
		source: source,
		target: target,
	}
}

// Import imports the databases from the source to the target Client. Provide
// opts to alter the import behaviour.
func (i *Importer) Import(ctx context.Context, opts ...ImportOption) error {
	cfg := importConfig{
		pingTimeout: defaultPingTimeout,
	}
	for _, opt := range opts {
		opt(&cfg)
	}

	if err := i.ping(ctx, cfg.pingTimeout); err != nil {
		return fmt.Errorf("ping: %w", err)
	}

	names, err := i.source.ListDatabaseNames(ctx, bson.M{})
	if err != nil {
		return fmt.Errorf("list database names: %w", err)
	}
	log.Println(fmt.Sprintf("Found databases: %v", names))
	names = cfg.filterDatabases(names...)
	log.Println(fmt.Sprintf("Filtered databases: %v", names))

	confirmed, err := cfg.confirm()
	if err != nil {
		return fmt.Errorf("confirm: %w", err)
	}
	if !confirmed {
		fmt.Println("Import aborted.")
		os.Exit(1)
	}

	group, ctx := errgroup.WithContext(ctx)
	for _, name := range names {
		name := name
		group.Go(func() error {
			db := i.source.Database(name)
			if err := cfg.dropDB(ctx, i.target.Database(name)); err != nil {
				return fmt.Errorf("drop %q database: %w", name, err)
			}
			if err := i.importDatabase(ctx, cfg, db); err != nil {
				return fmt.Errorf("import %q database: %w", name, err)
			}
			return nil
		})
	}

	if err := group.Wait(); err != nil {
		return err
	}

	return nil
}

func (i *Importer) ping(ctx context.Context, timeout time.Duration) error {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	if err := i.source.Ping(ctx, nil); err != nil {
		return fmt.Errorf("ping source: %w", err)
	}

	if err := i.target.Ping(ctx, nil); err != nil {
		return fmt.Errorf("ping target: %w", err)
	}

	return nil
}

func (i *Importer) importDatabase(ctx context.Context, cfg importConfig, db *mongo.Database) error {
	cfg.log(fmt.Sprintf("Import database: %v", db.Name()))

	names, err := db.ListCollectionNames(ctx, bson.M{})
	if err != nil {
		return fmt.Errorf("list collection names: %w", err)
	}
	cfg.log(fmt.Sprintf("[%s]: Found collections: %v", db.Name(), names))

	importCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	indexCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	group, ctx := errgroup.WithContext(importCtx)
	for _, name := range names {
		name := name
		group.Go(func() error {
			if err := i.importCollection(ctx, cfg, db.Collection(name)); err != nil {
				return fmt.Errorf("import %q collection: %w", name, err)
			}
			return nil
		})
	}

	if err := group.Wait(); err != nil {
		return err
	}

	if err := i.ensureIndexes(indexCtx, cfg, db, names); err != nil {
		return fmt.Errorf("ensure indexes: %w", err)
	}

	return nil
}

func (i *Importer) importCollection(ctx context.Context, cfg importConfig, col *mongo.Collection) error {
	cfg.log(fmt.Sprintf("[%s]: Import collection: %v", col.Database().Name(), col.Name()))

	target := i.target.Database(col.Database().Name()).Collection(col.Name())

	cur, err := col.Find(ctx, bson.M{})
	if err != nil {
		return fmt.Errorf("find all documents: %w", err)
	}
	defer cur.Close(ctx)

	for cur.Next(ctx) {
		doc := make(bson.M)
		if err := cur.Decode(&doc); err != nil {
			return fmt.Errorf("decode document: %w", err)
		}
		if _, err := target.InsertOne(ctx, doc); err != nil {
			return fmt.Errorf("insert document: %w", err)
		}
	}

	if err := cur.Err(); err != nil {
		return fmt.Errorf("cursor: %w", err)
	}

	return nil
}

func (i *Importer) ensureIndexes(ctx context.Context, cfg importConfig, db *mongo.Database, names []string) error {
	cfg.log(fmt.Sprintf("[%s]: Ensure indexes: %v", db.Name(), names))
	for _, name := range names {
		if err := i.ensureColIndexes(ctx, cfg, db.Collection(name)); err != nil {
			return fmt.Errorf("ensure indexes for %q collection: %w", name, err)
		}
	}
	return nil
}

func (i *Importer) ensureColIndexes(ctx context.Context, cfg importConfig, col *mongo.Collection) error {
	cfg.log(fmt.Sprintf("[%s]: Ensure collection indexes: %v", col.Database().Name(), col.Name()))

	target := i.target.Database(col.Database().Name()).Collection(col.Name())

	cur, err := col.Indexes().List(ctx)
	if err != nil {
		return fmt.Errorf("list indexes: %w", err)
	}

	var models []bson.M
	if err := cur.All(ctx, &models); err != nil {
		return fmt.Errorf("cursor: %w", err)
	}

	if err := cur.Err(); err != nil {
		return fmt.Errorf("cursor: %w", err)
	}

	idxModels := make([]mongo.IndexModel, 0, len(models))
	for i := range models {
		keym := models[i]["key"].(bson.M)
		keys := make(bson.D, 0)

		for k, v := range keym {
			if strings.HasPrefix(k, "_") {
				continue
			}
			keys = append(keys, bson.E{Key: k, Value: v})
		}

		if len(keys) == 0 {
			continue
		}

		name, _ := models[i]["name"].(string)
		unique, _ := models[i]["unique"].(bool)
		opts := options.Index().SetName(name).SetUnique(unique)

		idxModels = append(idxModels, mongo.IndexModel{
			Keys:    keys,
			Options: opts,
		})
	}

	if len(idxModels) > 0 {
		if _, err := target.Indexes().CreateMany(ctx, idxModels); err != nil {
			return fmt.Errorf("create indexes: %w", err)
		}
	}

	return nil
}

func (cfg importConfig) filterDatabases(names ...string) []string {
	if len(cfg.dbFilter) == 0 {
		return names
	}
	var filtered []string
L:
	for _, name := range names {
		for _, filter := range cfg.dbFilter {
			if !filter(name) {
				continue L
			}
		}
		filtered = append(filtered, name)
	}
	return filtered
}

func (cfg importConfig) dropDB(ctx context.Context, db *mongo.Database) error {
	if !cfg.drop {
		return nil
	}
	cfg.log(fmt.Sprintf("Dropping target database: %v", db.Name()))
	return db.Drop(ctx)
}

func (cfg importConfig) confirm() (bool, error) {
	if cfg.skipConfirm {
		return true, nil
	}
	fmt.Println()
	var confirmed bool
	if err := survey.AskOne(&survey.Confirm{
		Message: "Do you really want to import the above databases?",
		Default: false,
	}, &confirmed); err != nil {
		return confirmed, fmt.Errorf("survey: %w", err)
	}
	return confirmed, nil
}

func (cfg importConfig) log(v ...interface{}) {
	if cfg.verbose {
		log.Println(v...)
	}
}
