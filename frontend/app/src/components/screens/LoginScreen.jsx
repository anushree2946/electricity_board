import React, { useState } from "react";
import { Form, Button } from "react-bootstrap";
import axios from "axios";

function LoginScreen() {
    const [username, setUsername] = useState("");
  const [password, setPassword] = useState("");

  const handleSubmit = async(e) => {
    e.preventDefault();
    // Add your login logic here, such as sending a request to your backend server
    try {
        const response = await axios.post("/api/login/", {
          username,
          password,
        });
        console.log("Login success:", response.data);
        // Store user data in local storage
        localStorage.setItem("userData", JSON.stringify(response.data));
        // Redirect the user or perform any other actions upon successful login
      } catch (error) {
        console.error("Login error:", error.response.data);
        // Set error message for display
        // setError(error.response.data.message);
      }
      // Clear the form fields after submission
      setUsername("");
      setPassword("");
  };


  
  return (
    <div className="container">
    <h2>Login</h2>
    <Form onSubmit={handleSubmit}>
      <Form.Group controlId="formBasicUsername">
        <Form.Label>Username</Form.Label>
        <Form.Control
          type="text"
          placeholder="Enter username"
          value={username}
          onChange={(e) => setUsername(e.target.value)}
          required
        />
      </Form.Group>

      <Form.Group controlId="formBasicPassword">
        <Form.Label>Password</Form.Label>
        <Form.Control
          type="password"
          placeholder="Password"
          value={password}
          onChange={(e) => setPassword(e.target.value)}
          required
        />
      </Form.Group>

      <Button variant="primary" type="submit">
        Login
      </Button>
    </Form>
  </div>
  )
}

export default LoginScreen