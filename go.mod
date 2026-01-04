module project_sem

go 1.23.3

require internal/common v1.0.0 // indirect

replace internal/common => ./internal/common

require internal/receiver v1.0.0

replace internal/receiver => ./internal/receiver

require internal/zipper v1.0.0

replace internal/zipper => ./internal/zipper

require internal/csver v1.0.0

replace internal/csver => ./internal/csver

require internal/sqler v1.0.0

replace internal/sqler => ./internal/sqler

require internal/responder v1.0.0

require github.com/lib/pq v1.10.9 // indirect

replace internal/responder => ./internal/responder
