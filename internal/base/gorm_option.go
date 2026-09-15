package base

// QueryOption narrows a read. Column and preload names are validated before they reach SQL.
type QueryOption struct {
	Preloads  []string
	Selects   []string
	Omits     []string
	Order     string
	Unscoped  bool
	ForUpdate bool
}

// InsertOption controls association saving on create.
type InsertOption struct {
	FullSaveAssociations bool
	Omit                 []string
}

// AssociationOption updates a GORM association on Entity.
type AssociationOption[T any] struct {
	Entity          *T
	AssociationName string
	Values          []any
}
