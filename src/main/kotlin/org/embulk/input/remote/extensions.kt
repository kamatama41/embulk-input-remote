package org.embulk.input.remote

import org.embulk.config.ConfigSource
import org.embulk.config.TaskSource
import org.embulk.spi.Exec

inline fun <reified T : Any> ConfigSource.loadConfig() = loadConfig(T::class.java)!!

inline fun <reified T : Any> TaskSource.loadTask() = loadTask(T::class.java)!!

fun Any.getLogger() = Exec.getLogger(javaClass)!!
