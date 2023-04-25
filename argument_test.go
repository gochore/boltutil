package boltutil

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCondition(t *testing.T) {
	t.Run("ignoreIfExist Put", func(t *testing.T) {
		db := testDB(t, true)
		defer db.Close()
		person := &Person{
			Id:   "id1",
			Name: "name1",
		}
		assert.NoError(t, db.Put(person))

		person.Name = "name2"
		assert.NoError(t, db.Put(person, NewCondition().IgnoreIfExist(true)))
		require.NoError(t, db.Get(person))
		assert.Equal(t, "name1", person.Name)

		person.Name = "name3"
		assert.NoError(t, db.Put(person))
		require.NoError(t, db.Get(person))
		assert.Equal(t, "name3", person.Name)

		person.Name = "name4"
		assert.NoError(t, db.Put(person, NewCondition().IgnoreIfExist(false)))
		require.NoError(t, db.Get(person))
		assert.Equal(t, "name4", person.Name)
	})

	t.Run("failIfExist Put", func(t *testing.T) {
		db := testDB(t, true)
		defer db.Close()
		person := &Person{
			Id:   "id1",
			Name: "name1",
		}
		assert.NoError(t, db.Put(person))

		person.Name = "name2"
		assert.Error(t, db.Put(person, NewCondition().FailIfExist(true)))
		require.NoError(t, db.Get(person))
		assert.Equal(t, "name1", person.Name)

		person.Name = "name3"
		assert.NoError(t, db.Put(person))
		require.NoError(t, db.Get(person))
		assert.Equal(t, "name3", person.Name)

		person.Name = "name4"
		assert.NoError(t, db.Put(person, NewCondition().FailIfExist(false)))
		require.NoError(t, db.Get(person))
		assert.Equal(t, "name4", person.Name)
	})

	t.Run("ignoreIfNotExist Get", func(t *testing.T) {
		db := testDB(t, true)
		defer db.Close()
		person := &Person{
			Id:   "id1",
			Name: "name1",
		}

		assert.NoError(t, db.Get(person, NewCondition().IgnoreIfNotExist(true)))

		assert.Error(t, db.Get(person))

		assert.Error(t, db.Get(person, NewCondition().IgnoreIfNotExist(false)))
	})

	t.Run("failIfNotExist Put", func(t *testing.T) {
		db := testDB(t, true)
		defer db.Close()
		person := &Person{
			Id:   "id1",
			Name: "name1",
		}

		assert.Error(t, db.Put(person, NewCondition().FailIfNotExist(true)))
		require.Error(t, db.Get(person))

		assert.NoError(t, db.Put(person))
		require.NoError(t, db.Get(person))
	})

	t.Run("failIfNotExist Delete", func(t *testing.T) {
		db := testDB(t, true)
		defer db.Close()
		person := &Person{
			Id:   "id1",
			Name: "name1",
		}

		assert.Error(t, db.Delete(person, NewCondition().FailIfNotExist(true)))

		assert.NoError(t, db.Delete(person))
	})
}
