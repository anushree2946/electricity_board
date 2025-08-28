import React from "react";
import { useState, useEffect } from "react";
import { Row, Col, Button, ButtonGroup } from "react-bootstrap";
import { Link } from "react-router-dom";
import DatePicker from "react-datepicker";
import 'react-datepicker/dist/react-datepicker.css';
import "./Home.css"; // Assuming you have a separate CSS file

function Home() {
  const [data, setData] = useState([]);
  const [totalPages, setTotalPages] = useState(1);
  const [currentPage, setCurrentPage] = useState(1);
  const [startDate, setStartDate] = useState(null);
  const [endDate, setEndDate] = useState(null);
  const [searchQuery, setSearchQuery] = useState("");
  useEffect(() => {
   
    fetchData();
  }, [currentPage, startDate, endDate, searchQuery]);

  const fetchData = async () => {
    try {
      let url = `/api/getApplicantsData/?page=${currentPage}`;
      if(startDate && endDate){
        // console.log(startDate)
        // console.log(startDate.toISOString().split("T")[0])
        url += `&start_date=${startDate.toISOString().split("T")[0]}&end_date=${
          endDate.toISOString().split("T")[0]
        }`;
      }

      if(searchQuery){
        url+=`&search=${searchQuery}`
      }
      const response = await fetch(url);
      const jsonData = await response.json();
      setData(jsonData.data);
      setTotalPages(jsonData.total_pages); // Fixed this line
    } catch (error) {
      console.log("Error fetching data", error);
    }
  };

  const handleSearchChange = (event) => {
    setSearchQuery(event.target.value);
  };

  const handleStartDateChange = (date) => {
    setStartDate(date);
  };

  const handleEndDateChange = (date) => {
    setEndDate(date);
  };

  const handleFirstPageClick = () => {
    setCurrentPage(1);
  };

  const handleLastPageClick = () => {
    setCurrentPage(totalPages);
  };
  const handlePageClick = (page) => {
      setCurrentPage(page);
    };
  return (
    <>
      <div className="container mt-2">
        <h1>Applicant Details</h1>
        <hr />
        <Row>
          <p>Filter By Date of Application</p>
          <Col md={2}>
            <DatePicker
              selected={startDate}
              className="form-control date"
              onChange={handleStartDateChange}
              placeholderText="From Date"
            />
          </Col>
          <Col md={2}>
            <DatePicker
              selected={endDate}
              className="form-control date"
              onChange={handleEndDateChange}
              placeholderText="To Date"
            />
          </Col>

          <Col md={3}></Col>

          <Col md={5}>
            <input
              type="text"
              className="form-control"
              value={searchQuery}
              onChange={handleSearchChange}
              placeholder="Search By Applicant ID.."
            />
          </Col>
        </Row>
        <hr />
        <div className="table-container">
          <table className="table table-bordered">
            <thead className="sticky-header">
              <tr>
                <th>ID</th>
                <th>Applicant Name</th>
                <th>Gender</th>
                <th>District</th>
                <th>State</th>
                <th>Pincode</th>
                <th>Ownership</th>
                <th>Govt ID Type</th>
                <th>ID Number</th>
                <th>Category</th>
                <th>Load Applied</th>
                <th>Date of Application</th>
                <th>Status</th>
                <th>Reviewer ID</th>
                <th>Reviewer Name</th>
                <th>Reviewer Comments</th>
                <th>Edit</th>
              </tr>
            </thead>

            <tbody>
              {data?.map((connection) => (
                <tr key={connection.id}>
                  <td>{connection.id}</td>
                  <td>{connection.Applicant.Applicant_Name}</td>
                  <td>{connection.Applicant.Gender}</td>
                  <td>{connection.Applicant.District}</td>
                  <td>{connection.Applicant.State}</td>
                  <td>{connection.Applicant.Pincode}</td>
                  <td>{connection.Applicant.Ownership}</td>
                  <td>{connection.Applicant.GovtID_Type}</td>
                  <td>{connection.Applicant.ID_Number}</td>
                  <td>{connection.Applicant.Category}</td>
                  <td>{connection.Load_Applied}</td>
                  <td>{connection.Date_of_Application}</td>
                  <td>{connection.Status}</td>
                  <td>{connection.Reviewer_ID}</td>
                  <td>{connection.Reviewer_Name}</td>
                  <td>{connection.Reviewer_Comments}</td>
                  <td>
                    <Link
                      type="button"
                      to={`/editApplicant/${connection.id}`}
                      className="btn btn-outline-success btn-sm"
                    >
                      <i className="fa-solid fa-pen-to-square">Edit</i>
                    </Link>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>

         
        </div>
        <div className="container">
            <ul className="pagination">
              <li
                className={`page-item ${currentPage === 1 ? "disabled" : ""}`}
              >
                <button onClick={handleFirstPageClick} className="page-link">
                  Go to First
                </button>
              </li>
              {Array.from({ length: totalPages }).map((_, index) => (
                <li
                  key={index}
                  className={`page-item ${
                    currentPage === index + 1 ? "active" : ""
                  }`}
                >
                  <button
                    onClick={() => handlePageClick(index + 1)}
                    className="page-link"
                  >
                    {index + 1}
                  </button>
                </li>
              ))}
              <li
                className={`page-item ${
                  currentPage === totalPages ? "disabled" : ""
                }`}
              >
                <button onClick={handleLastPageClick} className="page-link">
                  Go to Last
                </button>
              </li>
            </ul>
          </div>
      </div>
    </>
  );
}

export default Home;
