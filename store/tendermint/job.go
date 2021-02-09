package tendermint

import (
	esStore "github.com/celer-network/eth-services/store"
	"github.com/celer-network/eth-services/store/models"
	"github.com/google/uuid"
	"github.com/vmihailenco/msgpack/v5"
)

var (
	prefixJob = []byte("job")
)

func (store *TMStore) GetJob(jobID uuid.UUID) (*models.Job, error) {
	var job models.Job
	err := get(store.nsJob, jobID[:], &job)
	if err != nil {
		return nil, err
	}
	return &job, nil
}

func (store *TMStore) PutJob(job *models.Job) error {
	return set(store.nsJob, job.ID[:], job)
}

func (store *TMStore) DeleteJob(jobID uuid.UUID) error {
	return store.nsJob.Delete(jobID[:])
}

func (store *TMStore) GetUnhandledJobIDs() ([]uuid.UUID, error) {
	var jobIDs []uuid.UUID
	iter, err := store.nsJob.Iterator(nil, nil)
	if err != nil {
		return nil, toCreateIterError(err)
	}
	defer iter.Close()
	for ; iter.Valid(); iter.Next() {
		value := iter.Value()
		var job models.Job
		err := msgpack.Unmarshal(value, &job)
		if err != nil {
			return nil, toDecodeTxError(err)
		}
		if job.State == models.JobStateUnhandled {
			jobIDs = append(jobIDs, job.ID)
		}
	}
	if len(jobIDs) == 0 {
		return nil, esStore.ErrNotFound
	}
	return jobIDs, nil
}
