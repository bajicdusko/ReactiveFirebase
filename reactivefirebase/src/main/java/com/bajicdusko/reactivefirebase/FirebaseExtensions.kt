package com.bajicdusko.reactivefirebase

import com.google.firebase.database.DataSnapshot
import com.google.firebase.database.DatabaseError
import com.google.firebase.database.DatabaseReference
import com.google.firebase.database.FirebaseDatabase
import com.google.firebase.database.Query
import com.google.firebase.database.ValueEventListener
import com.pastecan.data.exception.FirebaseDatabaseException
import com.pastecan.data.exception.RetrievedValueNullException
import io.reactivex.Observable
import io.reactivex.Single
import timber.log.Timber

private val IRRELEVANT = Any()

/**
 * Created by Bajic Dusko (www.bajicdusko.com) on 27.03.18.
 */
fun Any.asMap(): MutableMap<String, Any> {
    val map = mutableMapOf<String, Any>()
    var obj = this
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


/*
 * Firebase repository
 */
/**
 * This is is the extension method on [FirebaseDatabase] class for getting the value.
 * Method returns [Single<T>] and can be used to compose the Rx stream.
 *
 * [onDataSnapshot] function will be called when [ValueEventListener.onDataChange] is called.
 * [onDataSnapshot] function is an extension function itself on [DataSnapshot] class.
 * [onDataSnapshot] lambda implementation needs to operate over [DataSnapshot] instance.
 *
 * In case of [DatabaseError] from the Firebase, [Single] will emit an error of [FirebaseDatabaseException] type.
 * If by any case, [DataSnapshot] is NULL, [Single] will emit an [FirebaseDatabaseException] exception.
 * If [DataSnapshot] is not NULL, but fetched value from the [FirebaseDatabase] is NULL, [Single] will emit
 * an error of [RetrievedValueNullException] type
 *
 * After the emission of value or error, [ValueEventListener] will be removed from the Firebase reference.
 */
inline fun <T : Any> FirebaseDatabase.singleValue(reference: String, children: Array<out String>?,
    crossinline onDataSnapshot: DataSnapshot.() -> T?) = singleValue(reference, children,
    onDataSnapshot, { null })

/**
 * This is is the extension method on [FirebaseDatabase] class for getting the value.
 * Method returns [Single] and can be used to compose the Rx stream.
 *
 * [onDataSnapshot] function will be called when [ValueEventListener.onDataChange] is called.
 * [onDataSnapshot] function is an extension function itself on [DataSnapshot] class.
 * [onDataSnapshot] lambda implementation needs to operate over [DataSnapshot] instance.
 *
 * [defaultValue] function needs to return instance of type T and will be emitted in case od [onDataSnapshot] errors
 *
 * In case of [DatabaseError] from the Firebase, [Single] will emit an error of [FirebaseDatabaseException] type.
 * If by any case, [DataSnapshot] is NULL and there is no [defaultValue] defined (also NULL), [Single] will
 * emit an [FirebaseDatabaseException] exception.
 * If [DataSnapshot] is not NULL, but fetched value from the [FirebaseDatabase] is NULL, [Single] will emit
 * an error of [RetrievedValueNullException] type
 *
 * After the emission of value or error, [ValueEventListener] will be removed from the Firebase reference.
 */
inline fun <T : Any> FirebaseDatabase.singleValue(reference: String, children: Array<out String>?,
    crossinline onDataSnapshot: DataSnapshot.() -> T?,
    crossinline defaultValue: () -> T?): Single<T> =
    Single.create<T> {
        val firebaseReference = getReference(reference)
        firebaseReference.addListenerForSingleValueEvent(object : ValueEventListener {
            override fun onCancelled(dbError: DatabaseError?) {
                it.onError(
                    FirebaseDatabaseException.databaseIssue(dbError?.message, dbError?.details, reference,
                        children))
                firebaseReference.removeEventListener(this)
            }

            override fun onDataChange(dataSnapshot: DataSnapshot?) {
                val childDataSnapshot = dataSnapshot.childDataSnapshot(children)
                val defaultValue = defaultValue()
                if (childDataSnapshot != null) {
                    val retrievedValue = onDataSnapshot(childDataSnapshot)
                    if (retrievedValue == null) {
                        it.onError(RetrievedValueNullException(reference, children))
                    } else {
                        it.onSuccess(retrievedValue)
                    }
                } else if (defaultValue != null) {
                    it.onSuccess(defaultValue)
                } else {
                    it.onError(FirebaseDatabaseException.dataSnapshotIssue(reference, children))
                }
                firebaseReference.removeEventListener(this)
            }
        })
    }

/**
 * Returns an [Observable] which emits the value on each change on specified firebase object.
 * When [Observable] is disposed, completed or finished in error state, [ValueEventListener] will be
 * removed from the [DatabaseReference]
 *
 * Use [onDataSnapshot] to implement your logic of fetching the object from the provided [DataSnapshot]
 * In case that [DataSnapshot] is NULL, [Observable] will emit an [FirebaseDatabaseException]
 * If retrieved value from the [DataSnapshot] is NULL, [Observable] will emit an [RetrievedValueNullException]
 */
inline fun <T : Any> FirebaseDatabase.listenForChanges(reference: String,
    children: Array<out String>?,
    crossinline onDataSnapshot: DataSnapshot.() -> T?): Observable<T> = listenForChanges(reference,
    children, onDataSnapshot, { null })


/**
 * Returns an [Observable] which emits the value on each change on specified firebase object.
 * When [Observable] is disposed, completed or finished in error state, [ValueEventListener] will be
 * removed from the [DatabaseReference]
 *
 * Use [onDataSnapshot] to implement your logic of fetching the object from the provided [DataSnapshot]
 * In case that [DataSnapshot] is NULL, [Observable] will emit an [FirebaseDatabaseException]
 * If retrieved value from the [DataSnapshot] is NULL and [defaultValue] is NULL,
 * [Observable] will emit an [RetrievedValueNullException]
 */
inline fun <T : Any> FirebaseDatabase.listenForChanges(reference: String,
    children: Array<out String>?,
    crossinline onDataSnapshot: DataSnapshot.() -> T?,
    crossinline defaultValue: () -> T?): Observable<T> {

    val firebaseReference = getReference(reference)
    var valueListener: ValueEventListener? = null

    return Observable.create<T> {
        firebaseReference.addValueEventListener(object : ValueEventListener {
            override fun onCancelled(dbError: DatabaseError?) {
                it.onError(
                    FirebaseDatabaseException.databaseIssue(dbError?.message, dbError?.details, reference,
                        children))
            }

            override fun onDataChange(dataSnapshot: DataSnapshot?) {
                val childDataSnapshot = dataSnapshot.childDataSnapshot(children)
                val defaultValue = defaultValue()
                if (childDataSnapshot != null) {
                    val retrievedValue = onDataSnapshot(childDataSnapshot)
                    if (retrievedValue == null) {
                        it.onError(RetrievedValueNullException(reference, children))
                    } else {
                        it.onNext(retrievedValue)
                    }
                } else if (defaultValue != null) {
                    it.onNext(defaultValue)
                } else {
                    it.onError(FirebaseDatabaseException.dataSnapshotIssue(reference, children))
                }
            }
        }.also { valueListener = it })
    }
        .doOnDispose({ dispose(firebaseReference, valueListener) })
        .doOnComplete({ dispose(firebaseReference, valueListener) })
        .doOnError({ dispose(firebaseReference, valueListener) })
}

fun dispose(databaseReference: DatabaseReference, valueEventListener: ValueEventListener?) {
    if (valueEventListener != null) {
        databaseReference.removeEventListener(valueEventListener)
        Timber.d("ValueListener removed")
    }
}

/**
 * This is is the extension method on [FirebaseDatabase] class for getting the last added value.
 * Descending ordering is executed over [lastByField] value
 *
 * Method returns [Single<T>] and can be used to compose the Rx stream.
 *
 * [onDataSnapshot] function will be called when [ValueEventListener.onDataChange] is called.
 * [onDataSnapshot] function is an extension function itself on [DataSnapshot] class.
 * [onDataSnapshot] lambda implementation needs to operate over [DataSnapshot] instance.
 *
 * In case of [DatabaseError] from the Firebase, [Single] will emit an error of [FirebaseDatabaseException] type.
 * If by any case, [DataSnapshot] is NULL, [Single] will emit an [FirebaseDatabaseException] exception.
 * If [DataSnapshot] is not NULL, but fetched value from the [FirebaseDatabase] is NULL, [Single] will emit
 * an error of [RetrievedValueNullException] type
 *
 * After the emission of value or error, [ValueEventListener] will be removed from the Firebase reference.
 */
inline fun <T : Any> FirebaseDatabase.lastValue(reference: String,
    children: Array<out String>?,
    lastByField: String, crossinline onDataSnapshot: DataSnapshot.() -> T?): Single<T> =
    lastValue(reference, children, lastByField, onDataSnapshot, { null })

/**
 * This is is the extension method on [FirebaseDatabase] class for getting the last added value.
 * Descending ordering is executed over [lastByField] value
 *
 * Method returns [Single<T>] and can be used to compose the Rx stream.
 *
 * [onDataSnapshot] function will be called when [ValueEventListener.onDataChange] is called.
 * [onDataSnapshot] function is an extension function itself on [DataSnapshot] class.
 * [onDataSnapshot] lambda implementation needs to operate over [DataSnapshot] instance.
 *
 * [defaultValue] function needs to return instance of type T and will be emitted in case od [onDataSnapshot] errors
 *
 * In case of [DatabaseError] from the Firebase, [Single] will emit an error of [FirebaseDatabaseException] type.
 * If by any case, [DataSnapshot] is NULL and there is no [defaultValue] defined (also NULL), [Single] will
 * emit an [FirebaseDatabaseException] exception.
 * If [DataSnapshot] is not NULL, but fetched value from the [FirebaseDatabase] is NULL, [Single] will emit
 * an error of [RetrievedValueNullException] type
 *
 * After the emission of value or error, [ValueEventListener] will be removed from the Firebase reference.
 */
inline fun <T : Any> FirebaseDatabase.lastValue(reference: String,
    children: Array<out String>?,
    lastByField: String, crossinline onDataSnapshot: DataSnapshot.() -> T?,
    crossinline defaultValue: () -> T?): Single<T> =
    lastOrSortedValue(reference, children, lastByField, true, onDataSnapshot, defaultValue)

/**
 * This is is the extension method on [FirebaseDatabase] class for getting the result sorted in descending
 * order by [sortField] value
 *
 * Method returns [Single<T>] and can be used to compose the Rx stream.
 *
 * [onDataSnapshot] function will be called when [ValueEventListener.onDataChange] is called.
 * [onDataSnapshot] function is an extension function itself on [DataSnapshot] class.
 * [onDataSnapshot] lambda implementation needs to operate over [DataSnapshot] instance.
 *
 * In case of [DatabaseError] from the Firebase, [Single] will emit an error of [FirebaseDatabaseException] type.
 * If by any case, [DataSnapshot] is NULL, [Single] will emit an [FirebaseDatabaseException] exception.
 * If [DataSnapshot] is not NULL, but fetched value from the [FirebaseDatabase] is NULL, [Single] will emit
 * an error of [RetrievedValueNullException] type
 *
 * After the emission of value or error, [ValueEventListener] will be removed from the Firebase reference.
 */
inline fun <T : Any> FirebaseDatabase.sortValues(reference: String,
    children: Array<out String>?,
    sortField: String, crossinline onDataSnapshot: DataSnapshot.() -> T?): Single<T> =
    lastOrSortedValue(reference, children, sortField, false, onDataSnapshot, { null })

/**
 * This is is the extension method on [FirebaseDatabase] class for getting the result sorted in descending
 * order by [sortField] value
 *
 * Method returns [Single<T>] and can be used to compose the Rx stream.
 *
 * [onDataSnapshot] function will be called when [ValueEventListener.onDataChange] is called.
 * [onDataSnapshot] function is an extension function itself on [DataSnapshot] class.
 * [onDataSnapshot] lambda implementation needs to operate over [DataSnapshot] instance.
 *
 * [defaultValue] function needs to return instance of type T and will be emitted in case od [onDataSnapshot] errors
 *
 * In case of [DatabaseError] from the Firebase, [Single] will emit an error of [FirebaseDatabaseException] type.
 * If by any case, [DataSnapshot] is NULL and there is no [defaultValue] defined (also NULL), [Single] will
 * emit an [FirebaseDatabaseException] exception.
 * If [DataSnapshot] is not NULL, but fetched value from the [FirebaseDatabase] is NULL, [Single] will emit
 * an error of [RetrievedValueNullException] type
 *
 * After the emission of value or error, [ValueEventListener] will be removed from the Firebase reference.
 */
inline fun <T : Any> FirebaseDatabase.sortValues(reference: String,
    children: Array<out String>?,
    sortField: String, crossinline onDataSnapshot: DataSnapshot.() -> T?,
    crossinline defaultValue: () -> T?): Single<T> =
    lastOrSortedValue(reference, children, sortField, false, onDataSnapshot, defaultValue)

inline fun <T : Any> FirebaseDatabase.lastOrSortedValue(reference: String,
    children: Array<out String>?,
    lastByOrSortField: String, lastValue: Boolean,
    crossinline onDataSnapshot: DataSnapshot.() -> T?,
    crossinline defaultValue: () -> T?): Single<T> =
    Single.create<T> {
        val childDataReference = getReference(reference).childDataReference(children)
        if (childDataReference != null) {
            var lastValueQuery: Query = childDataReference.orderByChild(lastByOrSortField)
            if (lastValue) {
                lastValueQuery = lastValueQuery.limitToLast(1)
            }

            lastValueQuery.addListenerForSingleValueEvent(object : ValueEventListener {
                override fun onCancelled(dbError: DatabaseError?) {
                    it.onError(
                        FirebaseDatabaseException.databaseIssue(dbError?.message, dbError?.details,
                            reference, children))
                    lastValueQuery.removeEventListener(this)
                }

                override fun onDataChange(dataSnapshot: DataSnapshot?) {
                    val defaultValue = defaultValue()
                    if (dataSnapshot != null) {
                        val retrievedValue = onDataSnapshot(dataSnapshot)
                        if (retrievedValue == null) {
                            it.onError(RetrievedValueNullException(reference, children))
                        } else {
                            it.onSuccess(retrievedValue)
                        }
                    } else if (defaultValue != null) {
                        it.onSuccess(defaultValue)
                    } else {
                        it.onError(FirebaseDatabaseException.dataSnapshotIssue(reference, children))
                    }
                    lastValueQuery.removeEventListener(this)
                }
            })
        }
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
        val childDataReference = getReference(reference).childDataReference(children)
        if (childDataReference != null) {
            onDataReference(childDataReference)
        } else {
            throw FirebaseDatabaseException.databaseReferenceIssue(reference, children)
        }
    })

fun FirebaseDatabase.remove(reference: String, children: Array<out String>?): Single<Any> =
    Single.fromCallable {
        getReference(reference).childDataReference(children)?.removeValue()
        IRRELEVANT
    }

/**
 * Iterating through children of [DatabaseReference] and returning last found child [DatabaseReference]
 */
fun DatabaseReference?.childDataReference(children: Array<out String>?): DatabaseReference? {
    var tempReference = this
    children?.forEach {
        tempReference = tempReference.subDataReference(it)
    }

    return tempReference
}

fun DatabaseReference?.subDataReference(child: String): DatabaseReference? = this?.child(child)

/**
 * Iterating through children of [DataSnapshot] and returning last found child [DataSnapshot]
 */
fun DataSnapshot?.childDataSnapshot(children: Array<out String>?): DataSnapshot? {
    var tempSnapshot = this
    children?.forEach {
        tempSnapshot = tempSnapshot.subDataSnapshot(it)
    }

    return tempSnapshot
}

fun DataSnapshot?.subDataSnapshot(child: String): DataSnapshot? = this?.child(child)