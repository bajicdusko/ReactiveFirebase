package com.bajicdusko.reactivefirebase

import com.bajicdusko.reactivefirebase.exception.FirebaseDatabaseException
import com.bajicdusko.reactivefirebase.exception.RetrievedValueNullException
import com.google.firebase.database.DataSnapshot
import com.google.firebase.database.DatabaseError
import com.google.firebase.database.DatabaseReference
import com.google.firebase.database.FirebaseDatabase
import com.google.firebase.database.Query
import com.google.firebase.database.ValueEventListener
import io.reactivex.Observable
import io.reactivex.Single
import io.reactivex.SingleEmitter
import timber.log.Timber

private val IRRELEVANT = Any()

fun Any.asMap(): MutableMap<String, Any> {
  val map = mutableMapOf<String, Any>()
  val obj = this
  this.javaClass.declaredFields.forEach {
    if (it != null) {
      it.isAccessible = true
      val value = it.get(obj)
      if (value != null) {
        map.put(it.name, value)
      }
    }
  }
  return map
}

/**
 * This is is the extension method on [FirebaseDatabase] class for getting the value.
 * Method returns [Single] and can be used to compose the Rx stream.
 *
 * [getChildValueFn] function will be called when [ValueEventListener.onDataChange] is called.
 * [getChildValueFn] function is an extension function itself on [DataSnapshot] class.
 * [getChildValueFn] lambda implementation needs to operate over [DataSnapshot] instance.
 *
 * [defaultValue] function needs to return instance of type T and will be emitted in case od [getChildValueFn] errors
 *
 * In case of [DatabaseError] from the Firebase, [Single] will emit an error of [FirebaseDatabaseException] type.
 * If by any case, [DataSnapshot] is NULL and there is no [defaultValue] defined (also NULL), [Single] will
 * emit an [FirebaseDatabaseException] exception.
 * If [DataSnapshot] is not NULL, but fetched value from the [FirebaseDatabase] is NULL, [Single] will emit
 * an error of [RetrievedValueNullException] type
 *
 * After the emission of value or error, [ValueEventListener] will be removed from the Firebase reference.
 */
inline fun <T : Any> FirebaseDatabase.get(
  reference: String,
  children: Array<out String>?,
  crossinline getChildValueFn: DataSnapshot.() -> T?,
  defaultValue: T? = null
): Single<T> {
  return Single.create<T> { emitter ->
    val firebaseReference = getReference(reference)
    firebaseReference.addListenerForSingleValueEvent(object : ValueEventListener {
      override fun onCancelled(dbError: DatabaseError?) {
        emitter.onError(
          FirebaseDatabaseException.databaseIssue(
            dbError?.message,
            dbError?.details,
            reference,
            children
          )
        )
        firebaseReference.removeEventListener(this)
      }

      override fun onDataChange(dataSnapshot: DataSnapshot?) {
        val childSnapshot = dataSnapshot forChildren children
        childSnapshot?.let {
          val childValue = getChildValueFn(it)
          childValue?.let {
            emitter.onSuccess(childValue)
          } ?: emitter.onError(RetrievedValueNullException(reference, children))
        } ?: defaultValue?.let {
          emitter.onSuccess(it)
        } ?: emitter.onError(FirebaseDatabaseException.dataSnapshotIssue(reference, children))

        firebaseReference.removeEventListener(this)
      }
    })
  }
}


/**
 * Returns an [Observable] which emits the value on each change on specified firebase object.
 * When [Observable] is disposed, completed or finished in error state, [ValueEventListener] will be
 * removed from the [DatabaseReference]
 *
 * Use [getChildValueFn] to implement your logic of fetching the object from the provided [DataSnapshot]
 * In case that [DataSnapshot] is NULL, [Observable] will emit an [FirebaseDatabaseException]
 * If retrieved value from the [DataSnapshot] is NULL and [defaultValue] is NULL,
 * [Observable] will emit an [RetrievedValueNullException]
 */
inline fun <T : Any> FirebaseDatabase.listenForChanges(
  reference: String,
  children: Array<out String>?,
  crossinline getChildValueFn: DataSnapshot.() -> T?,
  defaultValue: T? = null
): Observable<T> {
  val firebaseReference = getReference(reference)

  var valueListener: ValueEventListener? = null

  return Observable.create<T> { emitter ->
    firebaseReference.addValueEventListener(object : ValueEventListener {
      override fun onCancelled(dbError: DatabaseError?) {
        emitter.onError(
          FirebaseDatabaseException.databaseIssue(
            dbError?.message,
            dbError?.details,
            reference,
            children
          )
        )
      }

      override fun onDataChange(dataSnapshot: DataSnapshot?) {
        val childSnapshot = dataSnapshot forChildren children
        childSnapshot?.let {
          val childValue = getChildValueFn(childSnapshot)
          childValue?.let {
            emitter.onNext(it)
          } ?: emitter.onError(RetrievedValueNullException(reference, children))
        } ?: defaultValue?.let {
          emitter.onNext(it)
        } ?: emitter.onError(FirebaseDatabaseException.dataSnapshotIssue(reference, children))
      }
    }.also { valueListener = it })
  }
    .doOnDispose({ dispose(firebaseReference, valueListener) })
    .doOnComplete({ dispose(firebaseReference, valueListener) })
    .doOnError({ dispose(firebaseReference, valueListener) })
}

fun dispose(databaseReference: DatabaseReference, valueEventListener: ValueEventListener?) {
  valueEventListener?.let {
    databaseReference.removeEventListener(it)
    Timber.d("ValueListener removed")
  }
}

/**
 * This is is the extension method on [FirebaseDatabase] class for getting the last added value.
 * Descending ordering is executed over [lastByField] value
 *
 * Method returns [Single<T>] and can be used to compose the Rx stream.
 *
 * [getChildValueFn] function will be called when [ValueEventListener.onDataChange] is called.
 * [getChildValueFn] function is an extension function itself on [DataSnapshot] class.
 * [getChildValueFn] lambda implementation needs to operate over [DataSnapshot] instance.
 *
 * [defaultValue] function needs to return instance of type T and will be emitted in case od [getChildValueFn] errors
 *
 * In case of [DatabaseError] from the Firebase, [Single] will emit an error of [FirebaseDatabaseException] type.
 * If by any case, [DataSnapshot] is NULL and there is no [defaultValue] defined (also NULL), [Single] will
 * emit an [FirebaseDatabaseException] exception.
 * If [DataSnapshot] is not NULL, but fetched value from the [FirebaseDatabase] is NULL, [Single] will emit
 * an error of [RetrievedValueNullException] type
 *
 * After the emission of value or error, [ValueEventListener] will be removed from the Firebase reference.
 */
inline fun <T : Any> FirebaseDatabase.lastValue(
  reference: String,
  children: Array<out String>?,
  lastByField: String,
  crossinline getChildValueFn: DataSnapshot.() -> T?,
  defaultValue: T? = null
): Single<T> {
  return lastOrSortedValue(reference, children, lastByField, true, getChildValueFn, defaultValue)
}

/**
 * This is is the extension method on [FirebaseDatabase] class for getting the result sorted in descending
 * order by [sortField] value
 *
 * Method returns [Single<T>] and can be used to compose the Rx stream.
 *
 * [getChildValueFn] function will be called when [ValueEventListener.onDataChange] is called.
 * [getChildValueFn] function is an extension function itself on [DataSnapshot] class.
 * [getChildValueFn] lambda implementation needs to operate over [DataSnapshot] instance.
 *
 * [defaultValue] function needs to return instance of type T and will be emitted in case od [getChildValueFn] errors
 *
 * In case of [DatabaseError] from the Firebase, [Single] will emit an error of [FirebaseDatabaseException] type.
 * If by any case, [DataSnapshot] is NULL and there is no [defaultValue] defined (also NULL), [Single] will
 * emit an [FirebaseDatabaseException] exception.
 * If [DataSnapshot] is not NULL, but fetched value from the [FirebaseDatabase] is NULL, [Single] will emit
 * an error of [RetrievedValueNullException] type
 *
 * After the emission of value or error, [ValueEventListener] will be removed from the Firebase reference.
 */
inline fun <T : Any> FirebaseDatabase.sortValues(
  reference: String,
  children: Array<out String>?,
  sortField: String,
  crossinline getChildValueFn: DataSnapshot.() -> T?,
  defaultValue: T? = null
): Single<T> {
  return lastOrSortedValue(reference, children, sortField, false, getChildValueFn, defaultValue)
}

inline fun <T : Any> FirebaseDatabase.lastOrSortedValue(
  reference: String,
  children: Array<out String>?,
  lastByOrSortField: String,
  lastValue: Boolean,
  crossinline getChildValueFn: DataSnapshot.() -> T?,
  defaultValue: T?
): Single<T> {
  return Single.create<T> { emitter ->
    val childReference = getReference(reference) forChildren children
    childReference?.let {
      var lastValueQuery: Query = it.orderByChild(lastByOrSortField)
      if (lastValue) {
        lastValueQuery = lastValueQuery.limitToLast(1)
      }

      lastValueQuery.addListenerForSingleValueEvent(object : ValueEventListener {
        override fun onCancelled(dbError: DatabaseError?) {
          emitter.onError(
            FirebaseDatabaseException.databaseIssue(
              dbError?.message,
              dbError?.details,
              reference,
              children
            )
          )

          lastValueQuery.removeEventListener(this)
        }

        override fun onDataChange(dataSnapshot: DataSnapshot?) {

          dataSnapshot?.let {
            val childValue = getChildValueFn(dataSnapshot)
            childValue?.let {
              emitter.onSuccess(it)
            } ?: emitter.onError(RetrievedValueNullException(reference, children))
          } ?: defaultValue?.let {
            emitter.onSuccess(it)
          } ?: emitter.onError(FirebaseDatabaseException.dataSnapshotIssue(reference, children))

          lastValueQuery.removeEventListener(this)
        }
      })
    } ?: emitter.onError(FirebaseDatabaseException.databaseReferenceIssue(reference, children))
  }
}

fun <T : Any> FirebaseDatabase.write(
  reference: String,
  children: Array<out String>?,
  value: T
): Single<T> {
  return write(reference, children, {
    setValue(value)
    value
  })
}

inline fun <T : Any> FirebaseDatabase.write(
  reference: String,
  children: Array<out String>?,
  crossinline writeValueFn: DatabaseReference.() -> T?
): Single<T> {
  return Single.fromCallable({
    val childReference = getReference(reference) forChildren children
    childReference?.let {
      writeValueFn(it)
    } ?: throw FirebaseDatabaseException.databaseReferenceIssue(reference, children)
  })
}
inline fun <T : Any> FirebaseDatabase.getByKey(reference: String, children: Array<out String>?, key: String,
    crossinline onDataSnapshot: DataSnapshot.() -> T?): Single<T> {
    return Single.create<T> {
        val dataReference = getReference(reference).childOrOriginalDataReference(children)
        if (dataReference == null) {
            it.onError(FirebaseDatabaseException.databaseReferenceIssue(reference, children))
        } else {
            val firstItemQuery = dataReference.orderByKey().equalTo(key).limitToFirst(1)
            firstItemQuery.addListenerForSingleValueEvent(object : ValueEventListener {
                override fun onCancelled(dbError: DatabaseError?) {
                    it.cancel(dbError, reference, children, {
                        firstItemQuery.removeEventListener(this)
                    })
                }

                override fun onDataChange(dataSnapshot: DataSnapshot?) {
                    it.readData(dataSnapshot, reference, children, onDataSnapshot, {
                        firstItemQuery.removeEventListener(this)
                    })
                }
            })
        }
    }
}

inline fun <T : Any> FirebaseDatabase.getByChildValue(reference: String, children: Array<out String>?, field: String,
    value: String, crossinline onDataSnapshot: DataSnapshot.() -> T?): Single<T> {
    return Single.create<T> {
        val dataReference = getReference(reference).childOrOriginalDataReference(children)

        if (dataReference == null) {
            it.onError(FirebaseDatabaseException.databaseReferenceIssue(reference, children))
        } else {
            val foundChildQuery = dataReference.orderByChild(field).equalTo(value).limitToFirst(1)
            foundChildQuery.addListenerForSingleValueEvent(object : ValueEventListener {
                override fun onCancelled(dbError: DatabaseError?) {
                    it.cancel(dbError, reference, children, {
                        foundChildQuery.removeEventListener(this)
                    })
                }

                override fun onDataChange(dataSnapshot: DataSnapshot?) {
                    it.readData(dataSnapshot, reference, children, onDataSnapshot, {
                        foundChildQuery.removeEventListener(this)
                    })
                }
            })
        }
    }
}

inline fun <T : Any> SingleEmitter<T>.cancel(dbError: DatabaseError?, reference: String, children: Array<out String>?,
    removeCallback: () -> Unit) {
    onError(FirebaseDatabaseException.databaseIssue(dbError?.message, dbError?.details, reference, children))
    removeCallback()
}

inline fun <T : Any> SingleEmitter<T>.readData(dataSnapshot: DataSnapshot?, reference: String,
    children: Array<out String>?, crossinline onDataSnapshot: DataSnapshot.() -> T?, removeCallback: () -> Unit) {
    if (dataSnapshot != null) {
        val retrievedValue = onDataSnapshot(dataSnapshot)
        if (retrievedValue == null) {
            onError(RetrievedValueNullException(reference,
                children))
        } else {
            onSuccess(retrievedValue)
        }
    } else {
        onError(FirebaseDatabaseException.dataSnapshotIssue(reference, children))
    }
    removeCallback()
}

fun <T : Any> FirebaseDatabase.writeValue(reference: String, children: Array<out String>?,
    value: T) =
    writeValue(reference, children, {
        setValue(value)
        value
    })

inline fun <T : Any> FirebaseDatabase.writeValue(reference: String, children: Array<out String>?,
    crossinline onDataReference: DatabaseReference.() -> T?): Single<T> =
    Single.fromCallable({
        val childDataReference = getReference(reference).childOrOriginalDataReference(children)
        if (childDataReference != null) {
            onDataReference(childDataReference)
        } else {
            throw FirebaseDatabaseException.databaseReferenceIssue(reference, children)
        }
    })

fun FirebaseDatabase.remove(
  reference: String,
  children: Array<out String>?
): Single<Any> {
  return Single.fromCallable {
    val childReference = getReference(reference) forChildren children
    childReference?.let {
      it.removeValue()
      IRRELEVANT
    } ?: throw FirebaseDatabaseException.databaseReferenceIssue(reference, children)
  }
}

/**
 * Iterating through children of [DatabaseReference] and returning last found child [DatabaseReference]
 * If there are no children, returning original DatabaseReference
 */
infix fun DatabaseReference?.forChildren(children: Array<out String>?): DatabaseReference? {
  var childReference = this
  children?.forEach {
    childReference = childReference forChild it
  }
fun DatabaseReference?.childOrOriginalDataReference(children: Array<out String>?): DatabaseReference? {
    var tempReference = this
    children?.forEach {
        tempReference = tempReference.subDataReference(it)
    }

  return childReference
}

infix fun DatabaseReference?.forChild(child: String): DatabaseReference? = this?.child(child)

/**
 * Iterating through children of [DataSnapshot] and returning last found child [DataSnapshot]
 * If there are no children, returning original DataSnapshot
 */
infix fun DataSnapshot?.forChildren(children: Array<out String>?): DataSnapshot? {
  var childSnapshot = this
  children?.forEach {
    childSnapshot = childSnapshot forChild it
  }
fun DataSnapshot?.childOrOriginalDataSnapshot(children: Array<out String>?): DataSnapshot? {
    var tempSnapshot = this
    children?.forEach {
        tempSnapshot = tempSnapshot.subDataSnapshot(it)
    }

  return childSnapshot
}

infix fun DataSnapshot?.forChild(child: String): DataSnapshot? = this?.child(child)