// Package migrations embeds the versioned schema migrations. Applied by the
// `migrate` subcommand via pkg/migratex (forward-only *.up.sql).
package migrations

import "embed"

//go:embed sql/*.sql
var FS embed.FS
