package com.pastecan.data.exception

/**
 * Created by Dusko Bajic on 25.06.17.
 * GitHub @bajicdusko
 */
class FirebaseDatabaseException(message: String, details: String) : Exception(
    "$message - $details") {

    companion object {
        fun databaseReferenceIssue(reference: String, children: Array<out String>?) =
            FirebaseDatabaseException(
                "Value could not be read. DatabaseReference is null.",
                "Exception occurred on reference: $reference and children: ${children?.forEach {
                    String.format("-> %s", it)
                }}.")

        fun dataSnapshotIssue(reference: String, children: Array<out String>?) =
            FirebaseDatabaseException(
                "Value could not be read. DataSnapshot is null and defaultValue is not provided",
                "Exception occurred on reference: $reference and children: ${children?.forEach {
                    String.format("-> %s", it)
                }}.")

        fun databaseIssue(message: String?, details: String?, reference: String,
            children: Array<out String>?) =
            FirebaseDatabaseException(
                message ?: "Database Error",
                details ?: "Exception occurred on reference: $reference and children: ${children?.forEach {
                    String.format("-> %s", it)
                }}.")
    }
}