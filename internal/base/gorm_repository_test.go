package base

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/glebarez/sqlite"
	"gorm.io/gorm"
)

type widget struct {
	SQLModel
	Name string
}

func TestGormRepositoryCRUDAndPaging(t *testing.T) {
	ctx := context.Background()
	db := openWidgetDB(t)
	repo := NewBaseGormRepository[widget, uint](db)

	item := &widget{Name: "alpha"}
	if err := repo.Create(ctx, item); err != nil {
		t.Fatal(err)
	}
	if item.ID == 0 {
		t.Fatal("expected generated id")
	}

	found, err := repo.GetByID(ctx, item.ID, QueryOption{})
	if err != nil {
		t.Fatal(err)
	}
	if found.Name != "alpha" {
		t.Fatalf("name = %s", found.Name)
	}

	if _, err := repo.GetByID(ctx, 999, QueryOption{}); !errors.Is(err, ErrNotFound) {
		t.Fatalf("missing row err = %v", err)
	}

	if err := repo.BulkCreate(ctx, []widget{{Name: "beta"}, {Name: "gamma"}}); err != nil {
		t.Fatal(err)
	}

	page, total, err := repo.Paging(ctx, nil, PagingInput{PageNum: 1, PageSize: 2, Sort: "id"}, QueryOption{})
	if err != nil {
		t.Fatal(err)
	}
	if total != 3 || len(page) != 2 {
		t.Fatalf("page len=%d total=%d", len(page), total)
	}

	if _, _, err := repo.Paging(ctx, nil, PagingInput{Sort: "name;drop"}, QueryOption{}); !errors.Is(err, ErrInvalidInput) {
		t.Fatalf("bad sort err = %v", err)
	}

	if err := repo.PatchUpdate(ctx, found, map[string]any{"name": "alpha-2"}); err != nil {
		t.Fatal(err)
	}
	renamed, err := repo.First(ctx, map[string]any{"name": "alpha-2"}, QueryOption{})
	if err != nil {
		t.Fatal(err)
	}
	if renamed.ID != item.ID {
		t.Fatalf("id = %d", renamed.ID)
	}

	if err := repo.Delete(ctx, item.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := repo.GetByID(ctx, item.ID, QueryOption{}); !errors.Is(err, ErrNotFound) {
		t.Fatalf("deleted err = %v", err)
	}
	soft, err := repo.GetByID(ctx, item.ID, QueryOption{Unscoped: true})
	if err != nil {
		t.Fatal(err)
	}
	if soft.DeletedAt.Time.IsZero() {
		t.Fatal("expected soft delete")
	}
	if err := repo.DeleteUnscoped(ctx, item.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := repo.GetByID(ctx, item.ID, QueryOption{Unscoped: true}); !errors.Is(err, ErrNotFound) {
		t.Fatalf("hard delete err = %v", err)
	}
}

func TestGormServiceRollsBack(t *testing.T) {
	ctx := context.Background()
	db := openWidgetDB(t)
	service := NewBaseService[widget, uint](db)

	err := service.InTx(ctx, func(repo IBaseGormRepository[widget, uint]) error {
		if err := repo.Create(ctx, &widget{Name: "rollback"}); err != nil {
			return err
		}
		return errors.New("fail")
	})
	if err == nil {
		t.Fatal("expected rollback error")
	}

	count, err := service.Count(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	if count != 0 {
		t.Fatalf("count = %d", count)
	}

	kept := &widget{Name: "kept"}
	if err := service.FirstOrCreate(ctx, kept, map[string]any{"name": "kept"}); err != nil {
		t.Fatal(err)
	}
	again := &widget{Name: "kept"}
	if err := service.FirstOrCreate(ctx, again, map[string]any{"name": "kept"}); err != nil {
		t.Fatal(err)
	}
	if again.ID != kept.ID {
		t.Fatalf("first-or-create ids %d and %d", kept.ID, again.ID)
	}

	body, err := service.Page(ctx, nil, PagingInput{PageNum: 1, PageSize: 10}, QueryOption{})
	if err != nil {
		t.Fatal(err)
	}
	if body.Count != 1 || body.HasNextPage {
		t.Fatalf("page = %+v", body)
	}
}

func TestSQLModelUUIDBeforeCreate(t *testing.T) {
	row := &SQLModelUUID{}
	if err := row.BeforeCreate(nil); err != nil {
		t.Fatal(err)
	}
	if row.ID == "" {
		t.Fatal("expected uuid")
	}
}

func openWidgetDB(t *testing.T) *gorm.DB {
	t.Helper()
	name := strings.NewReplacer("/", "_", " ", "_").Replace(t.Name())
	db, err := gorm.Open(sqlite.Open("file:"+name+"?mode=memory&cache=shared"), &gorm.Config{})
	if err != nil {
		t.Fatal(err)
	}
	sqlDB, err := db.DB()
	if err != nil {
		t.Fatal(err)
	}
	sqlDB.SetMaxOpenConns(1)
	t.Cleanup(func() { _ = sqlDB.Close() })
	if err := db.AutoMigrate(&widget{}); err != nil {
		t.Fatal(err)
	}
	return db
}
