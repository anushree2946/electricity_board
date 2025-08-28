import React, { useState, useEffect } from "react";
import { useParams } from "react-router-dom";
import { Row, Col, Form, Button, Container } from "react-bootstrap";
import { Link } from "react-router-dom";
import Messages from "../Messages";

function EditApplicant() {
  const { id } = useParams();
  const [applicantData, setApplicantData] = useState({});
  const [connectionData, setConnectionData] = useState({});
  const [message, setMessage] = useState("");


  const fetchApplicantData = async () => {
    try {
      const response = await fetch(`/api/update_applicant/${id}`);
      const data = await response.json();
      setApplicantData(data.applicant);
      setConnectionData(data.connection);
    } catch (error) {
      console.error("Error fetching applicant data:", error);
    }
  };

  useEffect(() => {
    fetchApplicantData();
  }, []);

  const handleChange = (e) => {
    const { name, value } = e.target;
    setApplicantData({ ...applicantData, [name]: value });
    setConnectionData({
      ...connectionData,
      [e.target.name]: e.target.value,
    });
  };





  const handleSubmit = async (e) => {
    e.preventDefault();
    try {
      if(connectionData.Load_Applied>200){
        setMessage("Load Applied cannot be greater than 200");

        setTimeout(()=>{
          setMessage("")
        },3000);
        return;
      }
      await fetch(`/api/update_applicant/${id}`, {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
        },
        body: JSON.stringify({
          applicant: applicantData,
          connection: connectionData,
        }),
      });

      setMessage("Applicant/Connection details has updated");
      setTimeout(()=>{
        setMessage("")
      },3000);
      return;
    
    } catch (error) {
      console.error("Error updating applicant data:", error);
    }
  };


  return (
    <>
      <Container>
        <div className="row">
          <div className="col-md-3">
            {" "}
            <Link to="/" className="btn btn-dark my-1">
              Go Back
            </Link>
          </div>
        </div>
        <hr />
        <h5>Edit Applicant Or Connection Details</h5>
        <hr />

        <Form onSubmit={handleSubmit}>
          {message && <Messages variant="info">{message}</Messages>}
          <Row>
            <Col md={6}>
              <Form.Group controlId="Applicant_Name">
                <Form.Label>Name</Form.Label>
                <Form.Control
                  type="text"
                  name="Applicant_Name"
                  value={applicantData.Applicant_Name || ""}
                  onChange={handleChange}
                  required
                />
              </Form.Group>


              <Form.Group controlId="Gender">
                <Form.Label>Gender</Form.Label>
                <Form.Control
                  as="select"
                  name="Gender"
                  value={applicantData.Gender || ""}
                  onChange={handleChange}
                  required
                >
                  <option value="Male">Male</option>
                  <option value="Female">Female</option>
                </Form.Control>
              </Form.Group>


              <Form.Group controlId="District">
                <Form.Label>District</Form.Label>
                <Form.Control
                  type="text"
                  name="District"
                  value={applicantData.District || ""}
                  onChange={handleChange}
                  required
                />
              </Form.Group>

              <Form.Group controlId="State">
                <Form.Label>State</Form.Label>
                <Form.Control
                  type="text"
                  name="State"
                  value={applicantData.State || ""}
                  onChange={handleChange}
                  required
                />
              </Form.Group>

              <Form.Group controlId="Pincode">
                <Form.Label>Pincode</Form.Label>
                <Form.Control
                  type="number"
                  name="Pincode"
                  value={applicantData.Pincode || ""}
                  onChange={handleChange}
                  required
                />
              </Form.Group>

              <Form.Group controlId="Ownership">
                <Form.Label>Ownership</Form.Label>
                <Form.Control
                  as="select"
                  name="Ownership"
                  value={applicantData.Ownership || ""}
                  onChange={handleChange}
                  required
                >
                  <option value="INDIVIDUAL">Individual</option>
                  <option value="JOINT">Joint</option>
                </Form.Control>
              </Form.Group>

              <Form.Group controlId="GovtID_Type">
                <Form.Label>Government ID Type</Form.Label>
                <Form.Control
                  as="select"
                  name="GovtID_Type"
                  value={applicantData.GovtID_Type || ""}
                  onChange={handleChange}
                  disabled
                  required
                >
                  <option value="AADHAR">Aadhar</option>
                  <option value="VOTER_ID">Voter ID</option>
                  <option value="PAN">PAN</option>
                  <option value="PASSPORT">Passport</option>
                </Form.Control>
              </Form.Group>

              <Form.Group controlId="ID_Number">
                <Form.Label>ID Number</Form.Label>
                <Form.Control
                  type="text"
                  name="ID_Number"
                  value={applicantData.ID_Number || ""}
                  onChange={handleChange}
                  required
                  disabled
                />
              </Form.Group>

            </Col>


            <Col md={6}>
            
            <Form.Group controlId="Category">
                <Form.Label>Category</Form.Label>
                <Form.Control
                  as="select"
                  name="Category"
                  value={applicantData.Category || ""}
                  onChange={handleChange}
                  required
                >
                  <option value="Residential">Residential</option>
                  <option value="Commercial">Commercial</option>
                </Form.Control>
              </Form.Group>

            
            
              <Form.Group controlId="Load_Applied">
                <Form.Label>Load Applied</Form.Label>
                <Form.Control
                  type="number"
                  name="Load_Applied"
                  value={connectionData.Load_Applied || ""}
                  onChange={handleChange}
                  required
                />
              </Form.Group>


              <Form.Group controlId="Date_of_Application">
                <Form.Label>Date of Application</Form.Label>
                <Form.Control
                  type="date"
                  name="Date_of_Application"
                  value={connectionData.Date_of_Application || ""}
                  onChange={handleChange}
                  required
                  disabled
                />
              </Form.Group>


              <Form.Group controlId="Date_of_Approval">
                <Form.Label>Date of Approval</Form.Label>
                <Form.Control
                  type="date"
                  name="Date_of_Approval"
                  value={connectionData.Date_of_Approval || ""}
                  onChange={handleChange}
                />
              </Form.Group>



              <Form.Group controlId="Status">
                <Form.Label>Status</Form.Label>
                <Form.Control
                  as="select"
                  name="Status"
                  value={connectionData.Status || ""}
                  onChange={handleChange}
                  required
                >
                  <option value="Connection Released">
                    Connection Released
                  </option>
                  <option value="Approved">Approved</option>
                  <option value="Pending">Pending</option>
                  <option value="Rejected">Rejected</option>
                </Form.Control>
              </Form.Group>




              <Form.Group controlId="Reviewer_ID">
                <Form.Label>Reviewer ID</Form.Label>
                <Form.Control
                  type="number"
                  name="Reviewer_ID"
                  value={connectionData.Reviewer_ID || ""}
                  onChange={handleChange}
                  required
                />
              </Form.Group>



              <Form.Group cont
              rolId="Reviewer_Name">
                <Form.Label>Reviewer Name</Form.Label>
                <Form.Control
                  type="text"
                  name="Reviewer_Name"
                  value={connectionData.Reviewer_Name || ""}
                  onChange={handleChange}
                  required
                />
              </Form.Group>



              <Form.Group controlId="Reviewer_Comments">
                <Form.Label>Reviewer Comments</Form.Label>
                <Form.Control
                  as="select" // Render as a select dropdown
                  name="Reviewer_Comments"
                  value={connectionData.Reviewer_Comments || ""}
                  onChange={handleChange}
                  required
                >
                  <option value="Installation pending">
                    Installation pending
                  </option>
                  <option value="Documents verification in progress">
                    Documents verification in progress
                  </option>
                  <option value="Installation completed">
                    Installation completed
                  </option>
                  <option value="KYC failed">KYC failed</option>
                </Form.Control>
              </Form.Group>


            </Col>
          </Row>
          <Button variant="primary" className="mt-3 text-center" type="submit">
            Update
          </Button>
          <br/>
          <br/>
          <br/>
        </Form>
      </Container>
    </>
  );
}

export default EditApplicant;
