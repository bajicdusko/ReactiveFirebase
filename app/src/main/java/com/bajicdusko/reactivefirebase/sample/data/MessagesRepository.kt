package com.bajicdusko.reactivefirebase.sample.data

import com.bajicdusko.reactivefirebase.FirebaseRepositoryData
import com.google.firebase.auth.FirebaseAuth
import com.google.firebase.database.FirebaseDatabase
import io.reactivex.Single

/**
 * Created by bajicdusko on 30/03/2018.
 */
class MessagesRepository(firebaseAuth: FirebaseAuth, firebaseDatabase: FirebaseDatabase) : FirebaseRepositoryData(
    firebaseAuth, firebaseDatabase) {

    fun getUserMessages(userId: String): Single<List<Message>> {
        return getOnAuthenticatedFirebase {
            getListValue(Message::class, "data", "messages", userId)
        }
    }
}