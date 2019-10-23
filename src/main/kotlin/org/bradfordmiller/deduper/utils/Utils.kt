package org.bradfordmiller.deduper.utils

/**
 * represents Either construct with [Left] and [Right] values
 */
sealed class Either<A, B>
/**
 *  @param A the [left] value in an [Either] construct
 *  @param B the right value in an [Either] construct
 */
data class Left<A, B>(val left: A) : Either<A, B>()
/**
 *  @param A the left value in an [Either] construct
 *  @param B the [right] value in an [Either] construct
 */
data class Right<A, B>(val right: B) : Either<A, B>()