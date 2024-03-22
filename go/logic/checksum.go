package logic

import (
	"github.com/github/gh-ost/go/sql"
	"time"
)

type checksum interface {
	buildCheckSQL(databaseName, originalTableName, ghostTableName string, sharedColumns []string, mappedSharedColumns []string, uniqueKey string, uniqueKeyColumns *sql.ColumnList, isTransactionalTable bool, checkColumnSize int) (string, []interface{}, error)
	getCreateTime() time.Time
}

type checksumChunk struct {
	min        *sql.ColumnValues
	max        *sql.ColumnValues
	includeMin bool

	checkSQLQuery string
	checkSQLArgs  []interface{}

	createTime time.Time
}

func newChecksumChunk(min, max *sql.ColumnValues, includeMin bool) *checksumChunk {
	return &checksumChunk{
		min:        min,
		max:        max,
		includeMin: includeMin,
		createTime: time.Now(),
	}
}

func (this *checksumChunk) buildCheckSQL(databaseName, originalTableName, ghostTableName string, sharedColumns []string, mappedSharedColumns []string, uniqueKey string, uniqueKeyColumns *sql.ColumnList, isTransactionalTable bool, checkColumnSize int) (string, []interface{}, error) {
	if this.checkSQLQuery != "" {
		return this.checkSQLQuery, this.checkSQLArgs, nil
	}
	var err error
	this.checkSQLQuery, this.checkSQLArgs, err = sql.BuildRangeCheckSumPreparedQuery(
		databaseName,
		originalTableName,
		ghostTableName,
		sharedColumns,
		mappedSharedColumns,
		uniqueKey,
		uniqueKeyColumns,
		this.min.AbstractValues(),
		this.max.AbstractValues(),
		this.includeMin,
		isTransactionalTable,
		checkColumnSize,
	)
	return this.checkSQLQuery, this.checkSQLArgs, err
}

func (this *checksumChunk) getCreateTime() time.Time {
	return this.createTime
}

type checksumDMLEvent struct {
	uniqueValue *sql.ColumnValues

	checkSQLQuery string
	checkSQLArgs  []interface{}

	createTime time.Time
}

func newChecksumDMLEvent() *checksumDMLEvent {
	return &checksumDMLEvent{}
}

func (this *checksumDMLEvent) buildCheckSQL(databaseName, originalTableName, ghostTableName string, sharedColumns []string, mappedSharedColumns []string, uniqueKey string, uniqueKeyColumns *sql.ColumnList, isTransactionalTable bool, checkColumnSize int) (string, []interface{}, error) {
	if this.checkSQLQuery != "" {
		return this.checkSQLQuery, this.checkSQLArgs, nil
	}
	return "", nil, nil
}

func (this *checksumDMLEvent) getCreateTime() time.Time {
	return this.createTime
}

type checksumDMLMergeEvent struct {
	uniqueValues []*sql.ColumnValues

	checkSQLQuery string
	checkSQLArgs  []interface{}

	createTime time.Time
}

func newChecksumDMLMergeEvent() *checksumDMLMergeEvent {
	return &checksumDMLMergeEvent{}
}

func (this *checksumDMLMergeEvent) buildCheckSQL(databaseName, originalTableName, ghostTableName string, sharedColumns []string, mappedSharedColumns []string, uniqueKey string, uniqueKeyColumns *sql.ColumnList, isTransactionalTable bool, checkColumnSize int) (string, []interface{}, error) {
	if this.checkSQLQuery != "" {
		return this.checkSQLQuery, this.checkSQLArgs, nil
	}
	return "", nil, nil
}

func (this *checksumDMLMergeEvent) getCreateTime() time.Time {
	return this.createTime
}
