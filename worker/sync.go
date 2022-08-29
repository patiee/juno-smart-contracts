package worker

import (
	"fmt"
	"sync"
	"time"

	"juno-contracts-worker/db"
	"juno-contracts-worker/db/model"
	"juno-contracts-worker/indexer"

	"github.com/google/uuid"
	"github.com/sirupsen/logrus"
)

const syncTableName = "sync"

type Service struct {
	db      *db.DB
	log     *logrus.Logger
	indexer *indexer.Service
}

func New(db *db.DB, l *logrus.Logger, i *indexer.Service) (*Service, error) {
	s := &Service{
		db:      db,
		log:     l,
		indexer: i,
	}

	return s, s.initSyncHeightTable()
}

func (s *Service) initSyncHeightTable() error {
	tableFields := map[string]interface{}{
		"name":    "TEXT",
		"height":  "NUMERIC",
		"hash":    "TEXT",
		"tx_hash": "TEXT",
		"index":   "INT",
		"sync":    "BOOLEAN",
		"err":     "TEXT",
	}
	uniqueIndexColums := []string{"name", "hash", "tx_hash", "index"}

	if err := s.db.CreateTable(syncTableName, tableFields); err != nil {
		return fmt.Errorf("could not create table %s: %w", syncTableName, err)
	}

	if err := s.db.CreateUniqueIndex(uniqueIndexColums, syncTableName+"_idx", syncTableName); err != nil {
		return fmt.Errorf("could not create table %s: %w", syncTableName, err)
	}

	return nil
}

func (s *Service) fetch(tableName string) error {
	s.log.Info("Fetching messages to process from table ", tableName)

	lastSync, err := s.fetchLastSync(tableName)
	if err != nil {
		return err
	}

	return s.fetchMessagesByHeight(tableName, lastSync)
}

func (s *Service) fetchLastSync(tableName string) (height int32, err error) {
	fields := []string{"height"}
	orderBy := map[string]string{
		"height":  "DESC",
		"tx_hash": "DESC",
		"index":   "DESC",
	}
	fieldsEqual := map[string]string{
		"name": fmt.Sprintf("'%s'", tableName),
	}
	limit := int32(1)
	qParams := &model.QParameters{
		OrderBy: &orderBy,
		Fields:  &fieldsEqual,
		Limit:   &limit,
	}
	rows, err := s.db.Select(syncTableName, fields, qParams)
	if err != nil {
		return 0, err
	}

	for rows.Next() {
		if err = rows.Scan(&height); err != nil {
			return 0, err
		}
	}

	return height, nil
}

func (s *Service) fetchFirstUnsync(tableName string) (*model.Unsync, error) {
	var u model.Unsync
	fields := []string{"id", "height", "hash", "tx_hash", "index"}
	orderBy := map[string]string{
		"height":  "ASC",
		"tx_hash": "ASC",
		"index":   "ASC",
	}
	fieldsEqual := map[string]string{
		"name": fmt.Sprintf("'%s'", tableName),
		"sync": "false",
	}
	limit := int32(1)
	qParams := &model.QParameters{
		OrderBy: &orderBy,
		Fields:  &fieldsEqual,
		Limit:   &limit,
	}
	rows, err := s.db.Select(syncTableName, fields, qParams)
	if err != nil {
		return nil, err
	}

	if rows.Next() {
		if err = rows.Scan(&u.ID, &u.Height, &u.Hash, &u.TxHash, &u.Index); err != nil {
			return nil, err
		}
		return &u, nil

	} else {
		return nil, nil
	}
}

func (s *Service) fetchMessagesByHeight(tableName string, startBlock int32) error {
	var height, index int32
	var hash, txHash string
	qFields := []string{"height", "hash", "tx_hash", "index"}
	qOrderBy := map[string]string{
		"height":  "ASC",
		"tx_hash": "ASC",
		"index":   "ASC",
	}
	qParams := &model.QParameters{
		OrderBy:    &qOrderBy,
		StartBlock: &startBlock,
	}

	rows, err := s.db.Select(tableName, qFields, qParams)
	if err != nil {
		return err
	}

	iFields := []string{"id", "name", "height", "hash", "tx_hash", "index", "sync"}

	for rows.Next() {
		if err = rows.Scan(&height, &hash, &txHash, &index); err != nil {
			return err
		}

		uuid, err := uuid.NewRandom()
		if err != nil {
			return err
		}

		iValues := []any{uuid, tableName, height, hash, txHash, index, false}

		if err = s.db.Insert(syncTableName, iFields, iValues); err != nil {
			return err
		}
	}

	return nil
}

func (s *Service) StartSync(wg *sync.WaitGroup, tableName string) {
	s.log.Info("Start processing ", tableName)
	defer wg.Done()

	if err := s.fetch(tableName); err != nil {
		s.log.Error("Error while fetching messages from table: ", tableName)
		return
	}

	for {
		firstUnsync, err := s.fetchFirstUnsync(tableName)
		if err != nil {
			s.log.Error("Error while fetching first unsync message from table: ", tableName)
			return
		}

		if firstUnsync == nil {
			s.log.Info("Finished processing all messages from ", tableName)
			time.Sleep(30 * time.Second)

			if err := s.fetch(tableName); err != nil {
				s.log.Error("Error while fetching messages from table: ", tableName)
				return
			}

			continue
		}

		var id, txHash, msg string
		var index int32
		fields := []string{"id", "index", "tx_hash", "msg"}

		qFields := map[string]string{
			"hash":    fmt.Sprintf("'%s'", firstUnsync.Hash),
			"height":  fmt.Sprintf("'%d'", firstUnsync.Height),
			"index":   fmt.Sprintf("'%d'", firstUnsync.Index),
			"tx_hash": fmt.Sprintf("'%s'", firstUnsync.TxHash),
		}
		qParams := &model.QParameters{Fields: &qFields}
		rows, err := s.indexer.QueryFields(tableName, fields, qParams)
		if err != nil {
			s.log.Errorf("could not query message from table %s tx_hash: %s index: %d err: %w", tableName, txHash, index, err)
			return
		}

		if rows.Next() {
			if err = rows.Scan(&id, &index, &txHash, &msg); err != nil {
				s.log.Errorf("could not read fields from table %s tx_hash: %s index: %d err: %w", tableName, txHash, index, err)
				return
			}

			if err = s.indexer.SaveJsonAsEntity(id, tableName[0:len(tableName)-1], msg); err != nil {
				s.log.Errorf("could not save entity from table %s tx_hash: %s index: %d err: %w", tableName, txHash, index, err)
				return
			}
		}

		if err = s.updateSync(firstUnsync.ID); err != nil {
			s.log.Errorf("could not update sync with id: %s from table %s tx_hash: %s index: %d err: %w", firstUnsync.ID, tableName, txHash, index, err)
			return
		}
	}
}

func (s *Service) updateSync(id string) error {
	qFields := map[string]string{
		"id": fmt.Sprintf("'%s'", id),
	}
	qParams := model.QParameters{
		Fields: &qFields,
	}
	return s.db.Update(qParams, syncTableName, "sync", "true")
}
