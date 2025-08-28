import React from 'react'
import {Alert} from 'react-bootstrap'

function Messages({variant,children}) {
  return (
    <Alert variant={variant}>{children}</Alert>
  )
}

export default Messages