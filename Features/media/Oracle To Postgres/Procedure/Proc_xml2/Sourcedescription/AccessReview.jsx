import { Box, Grid, TextField, Typography, styled } from '@material-ui/core'
import React, { useEffect, useState } from 'react';
import { makeStyles, withStyles } from '@material-ui/core/styles';
import TableBody from '@material-ui/core/TableBody';
import TableCell from '@material-ui/core/TableCell';
import Table from '@material-ui/core/Table';
import TableHead from '@material-ui/core/TableHead';
import TableRow from '@material-ui/core/TableRow';
import { useSelector } from 'react-redux';
import axios from "axios";
import config from "../../Config/config";


const useStyles = makeStyles((theme) => ({
    texttablecell: {
      overflowX: 'hidden',
      whiteSpace: "nowrap",
      width: "140px",
      overflow: "hidden",
      textOverflow: "ellipsis",
      // '&:hover': {
      //     overflow: 'visible'
      // }
    },
  
    table: {
      // minWidth: 150,
      width: '60%',
      height: '10%',
      border: '1px black'
    },
    root: {
      '& .MuiTextField-root': {
        margin: theme.spacing(1),
        width: '20ch',
      },
    },
    rootc: {
      '& > *': {
        margin: theme.spacing(1),
      },
    },
    input: {
      display: 'none',
    },
  }));


const StyledTableCell = withStyles((theme) => ({
head: {
    backgroundColor: '#3f51b5',
    color: theme.palette.common.white,
},
root: {
    padding: "0px 16px",
},

body: {
    fontSize: 14,
},
}))(TableCell);


const StyledTableRow = withStyles((theme) => ({
    root: {
      '&:nth-of-type(odd)': {
        backgroundColor: theme.palette.action.hover,
  
      },
  
      height: 10
  
    },
  }))(TableRow);


const useStylestable = makeStyles((theme) => ({
    table: {
      minWidth: 100,
      // width:10
    },
    formControl: {
      margin: theme.spacing(0),
      minWidth: 300,
  
    },
  
  }))


export default function AdminAccesslist() {
    const classes = useStyles();
    const classestable = useStylestable();
    const [isData, setIsData] = useState(true);
    const { details, createFeature, preview, editpreview, editPreviewdetails, headerValue } = useSelector(state => state.dashboardReducer);
    const [migtypeid, setMigtypeid] = useState(headerValue.title)
    const [objtype, setObjtype] = useState('Procedure')
    const [fnnames, setFnnames] = useState([])
    const [data, setData] = useState([])
  
  
    useEffect(() => {
      let sval = 0;
      if (headerValue) {
        if (headerValue.title === "Oracle TO Postgres") {
          sval = 1;
        } else if (headerValue.title === "SQLServer TO Postgres") {
          sval = 2;
        } else if (headerValue.title === "MYSQL TO Postgres") {
          sval = 3;
        }
      }
      let body = {
        "Object_Type": objtype,
        "Migration_TypeId": sval,
      };
      let conf = {
        headers: {
          Authorization: "Bearer " + config.ACCESS_TOKEN(),
        },
      };
      const form = new FormData();
      Object.keys(body).forEach((key) => {
        form.append(key, body[key]);
      });
      axios.post(`${config.API_BASE_URL()}/api/requestfndata/`, form, conf).then(
        (res) => {
          setFnnames(res.data)
          console.log(res.data)
        },
        (error) => {
          console.log(error);
        }
      );
    }, [objtype]);
  


    const handleversion = () => {

    }

    return (
    <>
        

        <Box py={2} px={2}>
            <Grid container xl={12} justifyContent="space-between" spacing={3}>
            <Grid item xs={12}>
                <Typography
                gutterBottom
                align='center'
                variant="h6"
                component="h2"
                className={classes.Object_Type}
                >
                Admin Users
                </Typography>
                <Table className={classestable.table} aria-label="customized table">
                <TableHead className={classes.primary}>
                    <TableRow>
                    <StyledTableCell align="left">User Email-ID</StyledTableCell>
                    <StyledTableCell align="left">Migration Type</StyledTableCell>
                    <StyledTableCell align="left">Roles</StyledTableCell>
                    </TableRow>
                </TableHead>
                <TableBody>


                    {isData ?
                    <StyledTableRow container>
                        <StyledTableCell item xl={8} >
                        <div className={classes.texttablecell}>
                            {"siva.n@quadrantresource.com"}
                        </div>
                        </StyledTableCell>
                        <StyledTableCell item xl={8} >
                        <div className={classes.texttablecell}>
                            {"Oracle TO Postgres"}
                        </div>
                        </StyledTableCell>
                        <StyledTableCell item xl={10} >
                        <div className={classes.texttablecell}>
                            {"User"}
                        </div>
                        </StyledTableCell>
                        


                    </StyledTableRow>
                    : <>
                        <StyledTableRow container>
                        <StyledTableCell align="center"></StyledTableCell>
                        <StyledTableCell align="center">No Requests</StyledTableCell>
                        <StyledTableCell align="center"></StyledTableCell>
                        </StyledTableRow>
                    </>
                    }


                </TableBody>
                </Table>
            </Grid>

            </Grid>
        </Box>
    </>
    )
}