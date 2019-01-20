package com.earthwave.core

import System.currentTimeMillis

object Profile {

  def profile[R](code: => R, t: Long = currentTimeMillis) = (code, currentTimeMillis - t)

}
