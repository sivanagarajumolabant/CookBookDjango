import { Box, Grid, TextField, Typography, styled } from '@material-ui/core'
import { Autocomplete } from '@material-ui/lab';
import Button from '@material-ui/core/Button';
import React, { useEffect, useState } from 'react';
import { makeStyles, withStyles } from '@material-ui/core/styles';
import TableBody from '@material-ui/core/TableBody';
import TableCell from '@material-ui/core/TableCell';
import Table from '@material-ui/core/Table';
import TableHead from '@material-ui/core/TableHead';
import TableRow from '@material-ui/core/TableRow';
import { TableContainer } from "@material-ui/core";


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

const StyledAutocomplete = styled(Autocomplete)({
    "& .MuiInputLabel-outlined:not(.MuiInputLabel-shrink)": {
        // Default transform is "translate(14px, 20px) scale(1)""
        // This lines up the label with the initial cursor position in the input
        // after changing its padding-left.
        transform: "translate(34px, 20px) scale(1);",
    },
    "& .MuiAutocomplete-inputRoot": {
        color: "black",
        // This matches the specificity of the default styles at https://github.com/mui-org/material-ui/blob/v4.11.3/packages/material-ui-lab/src/Autocomplete/Autocomplete.js#L90
        '&[class*="MuiOutlinedInput-root"] .MuiAutocomplete-input:first-child': {
            // Default left padding is 6px
            paddingLeft: 26,
            // height: '1rem'
        },
        "& .MuiOutlinedInput-notchedOutline": {
            borderColor: "grey",
        },
        "&:hover .MuiOutlinedInput-notchedOutline": {
            borderColor: "black",
        },
        "&.Mui-focused .MuiOutlinedInput-notchedOutline": {
            borderColor: "#3f51b5",
        },
    },
});

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
            width: '25ch',
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


export default function FeatureApprovals() {
    const classes = useStyles();
    const classestable = useStylestable();



    return (
        <>
            <Box py={1} px={1}>
                <Grid container direction='row' justifyContent='center'>
                    <Grid item>
                        <Typography variant='h6'>
                            Feature Requests
                        </Typography>
                    </Grid>
                </Grid>

            </Box>
            <Box py={2} px={2}>
                <Grid container xl={12} justifyContent="space-between" spacing={1}>
                    <Grid item xs={12}>
                        <TableContainer className={classestable.table}>
                            <Table stickyHeader aria-label="sticky table">
                                <TableHead className={classes.primary}>
                                    <TableRow>
                                        <StyledTableCell align="left">Project Version Id</StyledTableCell>
                                        <StyledTableCell align="left">Migration Type</StyledTableCell>
                                        <StyledTableCell align="left">Object Type</StyledTableCell>
                                        <StyledTableCell align="left">Feature Name</StyledTableCell>
                                        <StyledTableCell align="left">Feature Version Id</StyledTableCell>
                                        <StyledTableCell align="left">Approved Status</StyledTableCell>
                                        <StyledTableCell align="left">Request Created Date</StyledTableCell>
                                        <StyledTableCell align="left">Request Modified date</StyledTableCell>

                                    </TableRow>
                                </TableHead>
                                <TableBody>
                                    <StyledTableCell item xl={8}>
                                        <div className={classes.texttablecell}>
                                            {'1'}
                                        </div>
                                    </StyledTableCell>
                                    <StyledTableCell item xl={8}>
                                        <div className={classes.texttablecell}>
                                            {'Oracle to Postgres'}
                                        </div>
                                    </StyledTableCell>
                                    <StyledTableCell item xl={8}>
                                        <div className={classes.texttablecell}>
                                            {'Procedure'}
                                        </div>
                                    </StyledTableCell>
                                    <StyledTableCell item xl={8}>
                                        <div className={classes.texttablecell}>
                                            {'Xml'}
                                        </div>
                                    </StyledTableCell>
                                    <StyledTableCell item xl={8}>
                                        <div className={classes.texttablecell}>
                                            {'Version 1'}
                                        </div>
                                    </StyledTableCell>
                                    <StyledTableCell item xl={8}>
                                        <StyledTableCell item xl={8}>
                                            <Button
                                                type="button"
                                                size="small"
                                                variant="contained"
                                                color="primary"
                                                className={classes.submit}
                                                style={{ marginTop: '9px', fontSize: '9px', marginBottom: '8px' }}
                                            // onClick={() => handledeletesuperadmin(item.Email)}
                                            >
                                                Approve
                                            </Button>
                                        </StyledTableCell>
                                        <StyledTableCell item xl={4}>
                                            <Button
                                                type="button"
                                                size="small"
                                                variant="contained"
                                                color="primary"
                                                className={classes.submit}
                                                style={{ marginTop: '9px', fontSize: '9px', marginBottom: '8px' }}
                                            // onClick={() => handledeletesuperadmin(item.Email)}
                                            >
                                                Deny
                                            </Button>
                                        </StyledTableCell>
                                        <StyledTableCell item xl={4}>
                                            <Button
                                                type="button"
                                                size="small"
                                                variant="contained"
                                                color="primary"
                                                className={classes.submit}
                                                style={{ marginTop: '9px', fontSize: '9px', marginBottom: '8px' }}
                                            // onClick={() => handledeletesuperadmin(item.Email)}
                                            >
                                                Review
                                            </Button>
                                        </StyledTableCell>
                                    </StyledTableCell>
                                    <StyledTableCell item xl={4}>
                                        <div className={classes.texttablecell}>
                                            {'30-02-2022'}
                                        </div>
                                    </StyledTableCell>
                                    <StyledTableCell item xl={8}>
                                        <div className={classes.texttablecell}>
                                            {'05-04-2022'}
                                        </div>
                                    </StyledTableCell>
                                </TableBody>
                            </Table>
                            {/* <>
                                <StyledTableRow container>
                                    <StyledTableCell align="center"></StyledTableCell>
                                    <StyledTableCell align="center"></StyledTableCell>
                                    <StyledTableCell align="center"></StyledTableCell>
                                    <StyledTableCell align="center">No Requests</StyledTableCell>
                                    <StyledTableCell align="center"></StyledTableCell>
                                    <StyledTableCell align="center"></StyledTableCell>
                                </StyledTableRow>
                            </> */}
                        </TableContainer>
                    </Grid>
                </Grid>
            </Box>
            <Box py={1} px={1}>
                <Grid container direction='row' justifyContent='center'>
                    <Button
                        variant="contained"
                        color="primary"
                        component="span"
                        style={{ marginTop: 15 }}
                    >
                        {" "}
                        Delete Records
                    </Button>
                </Grid>
            </Box>

        </>
    )
}