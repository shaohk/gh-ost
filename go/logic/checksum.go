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
	uniqueMin  *sql.ColumnValues
	uniqueMax  *sql.ColumnValues
	includeMin bool

	checkSQLQuery string
	checkSQLArgs  []interface{}

	createTime time.Time
}

func newChecksumChunk(min, max *sql.ColumnValues, includeMin bool) *checksumChunk {
	return &checksumChunk{
		uniqueMin:  min,
		uniqueMax:  max,
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
		this.uniqueMin.AbstractValues(),
		this.uniqueMax.AbstractValues(),
		this.includeMin,
		isTransactionalTable,
		checkColumnSize,
	)
	return this.checkSQLQuery, this.checkSQLArgs, err
}

func (this *checksumChunk) getCreateTime() time.Time {
	return this.createTime
}

type checksumDMLEvents struct {
	uniqueValues [][]interface{}

	checkSQLQuery string
	checkSQLArgs  []interface{}

	createTime time.Time
}

func newChecksumDMLEvents(values [][]interface{}) *checksumDMLEvents {
	return &checksumDMLEvents{
		uniqueValues: values,
		createTime:   time.Now(),
	}
}

func (this *checksumDMLEvents) buildCheckSQL(databaseName, originalTableName, ghostTableName string, sharedColumns []string, mappedSharedColumns []string, uniqueKey string, uniqueKeyColumns *sql.ColumnList, isTransactionalTable bool, checkColumnSize int) (string, []interface{}, error) {
	if this.checkSQLQuery != "" {
		return this.checkSQLQuery, this.checkSQLArgs, nil
	}
	var err error
	this.checkSQLQuery, this.checkSQLArgs, err = sql.BuildEnumCheckSumPreparedQuery(
		databaseName,
		originalTableName,
		ghostTableName,
		sharedColumns,
		mappedSharedColumns,
		uniqueKey,
		uniqueKeyColumns,
		this.uniqueValues,
		isTransactionalTable,
		checkColumnSize,
	)

	return this.checkSQLQuery, this.checkSQLArgs, err
}

func (this *checksumDMLEvents) getCreateTime() time.Time {
	return this.createTime
}
