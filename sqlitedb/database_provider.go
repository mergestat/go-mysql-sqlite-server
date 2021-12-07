package sqlitedb

import (
	"fmt"
	"sort"
	"strings"
	"sync"

	"crawshaw.io/sqlite/sqlitex"
	"github.com/dolthub/go-mysql-server/sql"
)

var _ sql.DatabaseProvider = &provider{}
var _ sql.MutableDatabaseProvider = &provider{}

type provider struct {
	mut       sync.RWMutex
	databases map[string]*Database
}

func NewProvider(dbs ...sql.Database) *provider {
	databases := make(map[string]*Database, len(dbs))
	for _, db := range dbs {
		db, ok := db.(*Database)
		if !ok {
			continue
		}
		databases[strings.ToLower(db.Name())] = db
	}
	return &provider{
		databases: databases,
	}
}

func (p *provider) Database(name string) (sql.Database, error) {
	p.mut.RLock()
	defer p.mut.RUnlock()
	name = strings.ToLower(name)

	if db, ok := p.databases[name]; !ok {
		return nil, sql.ErrDatabaseNotFound.New()
	} else {
		return db, nil
	}
}

func (p *provider) HasDatabase(name string) bool {
	p.mut.RLock()
	defer p.mut.RUnlock()
	name = strings.ToLower(name)

	_, ok := p.databases[name]
	return ok
}

func (p *provider) AllDatabases() []sql.Database {
	p.mut.RLock()
	defer p.mut.RUnlock()

	all := make([]sql.Database, len(p.databases))
	var i int
	for _, db := range p.databases {
		all[i] = db
		i++
	}

	sort.Slice(all, func(i, j int) bool {
		return all[i].Name() < all[j].Name()
	})

	return all
}

func (p *provider) CreateDatabase(ctx *sql.Context, name string) error {
	p.mut.Lock()
	defer p.mut.Unlock()
	name = strings.ToLower(name)

	pool, err := sqlitex.Open(fmt.Sprintf("file:%s?mode=memory&cache=shared", name), 0, 10)
	if err != nil {
		return err
	}
	p.databases[name] = NewDatabase(name, pool, nil)
	return nil
}

func (p *provider) DropDatabase(ctx *sql.Context, name string) error {
	p.mut.Lock()
	defer p.mut.Unlock()
	name = strings.ToLower(name)

	delete(p.databases, name)
	return nil
}
