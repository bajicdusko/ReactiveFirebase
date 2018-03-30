package com.bajicdusko.reactivefirebase.sample.data

import com.bajicdusko.reactivefirebase.FirebaseRepositoryData
import com.google.firebase.auth.FirebaseAuth
import com.google.firebase.database.FirebaseDatabase
import io.reactivex.Observable
import io.reactivex.Single

/**
 * Created by bajicdusko on 30/03/2018.
 */

/**
 * For the sake of example, let's imagine that Firebase structure is something like this
 *
 * "data": {
 *   "users": [{
 *      "id": "userId_1",
 *      "displayName": "John Smith
 *   }, {
 *      "id": "userId_2",
 *      "displayName": "Jane Smith
 *   }],
 *   "messages": {
 *     "userId_1": [{
 *       "from": "userId_1",
 *       "to": "userId_2",
 *       "message": "Hello User 2"
 *     }],
 *     "userId_2": [{
 *       "from": "userId_2",
 *       "to": "userId_1",
 *       "message": "Hello User 1"
 *     }]
 *   }
 * }
 *
 *
 *
 *
 */
class UserFirebaseRepository(firebaseAuth: FirebaseAuth, firebaseDatabase: FirebaseDatabase) : FirebaseRepositoryData(
    firebaseAuth, firebaseDatabase) {

    fun getUsers(): Single<List<User>> = getOnAuthenticatedFirebase { getListValue(User::class, "users") }

    fun getUserUpdates(): Observable<List<User>> = observeOnAuthenticatedFirebase {
        listenForListChanges(User::class, "users")
    }
}