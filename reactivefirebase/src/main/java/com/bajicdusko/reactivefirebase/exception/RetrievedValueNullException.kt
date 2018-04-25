package com.bajicdusko.reactivefirebase.exception

class RetrievedValueNullException(reference: String, children: Array<out String>?) : Exception(
    "Exception occurred on reference: $reference and children: ${children?.forEach {
        String.format("-> %s", it)
    }}.")