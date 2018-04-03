package com.bajicdusko.reactivefirebase

import com.bajicdusko.reactivefirebase.exception.FirebaseUnknownSignInException
import com.bajicdusko.reactivefirebase.exception.RetrievedValueNullException
import com.google.firebase.auth.FacebookAuthProvider
import com.google.firebase.auth.FirebaseAuth
import com.google.firebase.auth.GoogleAuthProvider
import com.google.firebase.database.FirebaseDatabase
import io.reactivex.Observable
import io.reactivex.Single
import kotlin.reflect.KClass

/**
 * Created by Bajic Dusko (www.bajicdusko.com) on 27.03.18.
 */
open class FirebaseRepositoryData constructor(val firebaseAuth: FirebaseAuth, val db: FirebaseDatabase) {

    private fun login(): Single<Boolean> {
        return isLoggedIn()
            .flatMap { if (!it) saveLoginCredentials("") else Single.just(it) }
    }

    protected fun <T> getOnAuthenticatedFirebase(fn: () -> Single<T>): Single<T> {
        return login().flatMap { fn() }
    }

    protected fun <T> observeOnAuthenticatedFirebase(fn: () -> Observable<T>): Observable<T> {
        return login().flatMapObservable { fn() }
    }

    fun isLoggedIn(): Single<Boolean> = Single.fromCallable({
        firebaseAuth.currentUser != null
    })

    fun saveLoginCredentials(googleSignInIdToken: String?): Single<Boolean> =
        Single.create<Boolean> { emitter ->
            try {
                firebaseAuth.signInWithCredential(
                    GoogleAuthProvider.getCredential(googleSignInIdToken, null))
                    .addOnFailureListener { emitter.onError(it) }
                    .addOnSuccessListener { emitter.onSuccess(true) }
            } catch (ex: Exception) {
                emitter.onError(ex)
            }
        }

    fun saveFacebookLoginCredentials(facebookLoginAccessToken: String): Single<Boolean> =
        Single.create<Boolean> { emitter ->
            try {
                firebaseAuth.signInWithCredential(
                    FacebookAuthProvider.getCredential(facebookLoginAccessToken))
                    .addOnCompleteListener {
                        if (it.isSuccessful) {
                            emitter.onSuccess(true)
                        } else {
                            emitter.onError(it?.exception ?: FirebaseUnknownSignInException())
                        }
                    }
            } catch (ex: Exception) {
                emitter.onError(ex)
            }
        }

    fun existInDatabase(reference: String, vararg children: String): Single<Boolean> =
        db.singleValue(reference, children, { exists() }, { false })


    fun <T : Any> getValue(typeClass: KClass<T>, reference: String,
        vararg children: String): Single<T> =
        db.singleValue(reference, children, { getValue(typeClass.java) })

    fun <T : Any> getListValue(typeClass: KClass<T>, reference: String,
        vararg children: String): Single<List<T>> {
        return db.singleValue(reference, children, {
            val mutableList = mutableListOf<T>()
            this.children.forEach {
                val value = it.getValue(typeClass.java)
                if (value != null) {
                    mutableList.add(value)
                }
            }
            mutableList
        }, { emptyList<T>() })
    }

    fun <T : Any> getMapValue(listItemTypeClass: KClass<T>, reference: String,
        vararg children: String): Single<Map<String, List<T>>> {
        return db.singleValue(reference, children, {
            val map = HashMap<String, List<T>>()

            this.children.forEach { mapKey ->
                val list = mutableListOf<T>()
                mapKey.children.forEach {
                    val value = it.getValue(listItemTypeClass.java)
                    if (value != null) {
                        list.add(value)
                    }
                }

                map[mapKey.key] = list
            }

            map
        }, { HashMap() })
    }

    fun <T : Any> getLastValue(typeClass: KClass<T>, reference: String, lastByField: String,
        vararg children: String): Single<T> =
        db.lastValue(reference, children, lastByField, { getValue(typeClass.java) })

    fun <T : Any> getSortedListValue(typeClass: KClass<T>, reference: String,
        sortField: String, vararg children: String): Single<List<T>> =
        db.sortValues(reference, children, sortField, {
            val mutableList = mutableListOf<T>()
            this.children.forEach {
                val value = it.getValue(typeClass.java)
                if (value != null) {
                    mutableList.add(value)
                }
            }
            mutableList
        }, { emptyList<T>() })

    fun <T : Any> getByValue(typeClass: KClass<T>, reference: String, field: String, value: String,
        vararg children: String): Single<T> {
        return db.getByChildValue(reference, children, field, value, {
            if (this.childrenCount > 0) {
                this.children.first().getValue(typeClass.java)
            } else {
                throw RetrievedValueNullException(reference, children)
            }
        })
    }

    fun <T : Any> listenForChanges(typeClass: KClass<T>, reference: String,
        vararg children: String): Observable<T> =
        db.listenForChanges(reference, children, { getValue(typeClass.java) })

    fun <T : Any> listenForListChanges(typeClass: KClass<T>, reference: String,
        vararg children: String): Observable<List<T>> {
        return db.listenForChanges(reference, children, {
            val mutableList = mutableListOf<T>()
            this.children.forEach {
                val value = it.getValue(typeClass.java)
                if (value != null) {
                    mutableList.add(value)
                }
            }
            mutableList
        })
    }

    fun <T : Any> writeValue(value: T, reference: String,
        vararg children: String): Single<T> = db.writeValue(reference, children, value)

    fun <T : Any> writeValueToListId(reference: String, vararg children: String,
        getModel: (String) -> T): Single<T> =
        db.writeValue(reference, children, {
            val key = push().key
            val model = getModel(key)
            val map = mapOf(key to model.asMap())
            updateChildren(map)
            model
        })

    fun <T : Any> writeValueToList(value: T, reference: String,
        vararg children: String): Single<T> =
        db.writeValue(reference, children, {
            val key = push().key
            val map = mapOf(key to value.asMap())
            db.reference.updateChildren(map)
            value
        })

    fun removeValue(reference: String, vararg children: String) =
        db.remove(reference, children)
}