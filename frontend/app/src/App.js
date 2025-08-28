import React from 'react'
import { BrowserRouter, Routes, Route } from "react-router-dom";
import Home from './components/screens/Home';
import Header from './components/Header';
import EditApplicant from './components/screens/EditApplicant';
import Stats from './components/screens/Stats';
import LoginScreen from './components/screens/LoginScreen';



function App() {
  return (
    <>
    <BrowserRouter>
    <Header/>
    <Routes>
      <Route exact path="/" element={<Home/>}></Route>
    </Routes>
    <Routes>
      <Route exact path="/editApplicant/:id" element={<EditApplicant/>}></Route>
    </Routes>
    <Routes>
      <Route exact path="/StatisticsCollection/" element={<Stats/>}></Route>
    </Routes>
    <Routes>
      <Route exact path="/login" element={<LoginScreen/>}></Route>
    </Routes>
    
    
    
    </BrowserRouter>
    </>
  )
}

export default App