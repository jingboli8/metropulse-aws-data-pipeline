"""Stable exception types for AWS adapter failures."""


class ProcessingError(RuntimeError):
    """One object could not reach a valid completion marker."""


class ObjectIdentityMismatch(ProcessingError):
    """The event identity does not match the object currently read."""


class CompletionMarkerInvalid(ProcessingError):
    """A completion marker or its referenced outputs are inconsistent."""


class ConcurrentProcessing(ProcessingError):
    """Another invocation owns an unexpired conditional claim."""
