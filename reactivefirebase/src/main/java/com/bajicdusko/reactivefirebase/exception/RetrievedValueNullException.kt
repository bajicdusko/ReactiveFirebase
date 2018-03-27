package com.pastecan.data.exception

class RetrievedValueNullException(reference: String, children: Array<out String>?) : Exception(
    "Exception occurred on reference: $reference and children: ${children?.forEach {
        String.format("-> %s", it)
    }}.")