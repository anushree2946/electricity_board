import React from "react";
import { Link } from "react-router-dom";
import { LinkContainer } from "react-router-bootstrap";



function Header() {
  const isLoggedIn = localStorage.getItem("userData");
  return (
    <>
      <nav className="navbar navbar-expand-lg bg-primary" data-bs-theme="dark">
        <div className="container-fluid">
          <LinkContainer to="/">
            <Link className="navbar-brand text-white">Electricity Board</Link>
          </LinkContainer>
          <button
            className="navbar-toggler"
            type="button"
            data-bs-toggle="collapse"
            data-bs-target="#navbarColor01"
            aria-controls="navbarColor01"
            aria-expanded="false"
            aria-label="Toggle navigation"
          >
            <span className="navbar-toggler-icon"></span>
          </button>
          <div className="collapse navbar-collapse" id="navbarColor01">
            <ul className="navbar-nav me-auto">
              <li className="nav-item">
                <LinkContainer to="/">
                  <Link className="nav-link active  text-white">Home</Link>
                </LinkContainer>
              </li>

              <li className="nav-item">
                <LinkContainer to="/StatisticsCollection">
                  <Link className="nav-link  text-white">
                    Dashboard Statistics
                  </Link>
                </LinkContainer>
              </li>

              {isLoggedIn ? (
                <li className="nav-item">
                  <LinkContainer to="/logout">
                    <Link className="nav-link">Logout</Link>
                  </LinkContainer>
                </li>
              ) : (
                <li className="nav-item">
                  <LinkContainer to="/login">
                    <Link className="nav-link">Login</Link>
                  </LinkContainer>
                </li>
              )}




            </ul>
          </div>
        </div>
      </nav>
    </>
  );
}

export default Header;
