import io
import math
import datetime

def write_precip_header(
    file_object,
    project_short_name,
    project_long_name,
    version,
    organization,
    tech_support_email,
    contact_name,
    contact_email,
    model_name=None,
    disclaimer_text=None
):
    '''
    Generate the file header for the IWFM precip file

    Parameters
    ----------
    file_object : io.IOBase
        open file object to write output

    project_short_name : str
        short name for model application
    
    project_long_name : str
        long name for model application

    version : str
        version of the model application

    organization : str
        name of organization developing model

    tech_support_email : str
        email address for technical support

    contact_name : str
        name of model contact

    contact_email : str
        email address for model contact

    model_name : str or None, default None
        same as project long name if not provided

    disclaimer_text : str or None, default None
        model disclaimer. not written if None.

    Returns
    -------
    None
        writes to file_object
    '''
    description = 'This data file contains the time-series rainfall at each rainfall station used in the model.'

    if model_name is None:
        model_name = project_long_name

    file_object.write(iwfm_file_header())
    
    file_object.write(file_and_project_info(
        project_short_name, 
        project_long_name, 
        file_object.name, 
        version
    ))
    
    if disclaimer_text is not None:
        file_object.write(disclaimer(disclaimer_text))
    
    file_object.write(license_and_contact_info(
        model_name, 
        organization, 
        tech_support_email, 
        contact_name, 
        contact_email
        ))
    file_object.write(file_description(description))

def write_precip_specs(file_object, NRAIN, FACTRN, NSPRN, NFQRN, DSSFL):
    '''
    Format precipitation specifications information and data header to
    write to output text file.

    Parameters
    ----------
    file_object : io.IOBase
        open file object to write output

    NRAIN : int
        number of rainfall stations used in the model

    FACTRN : float
        unit conversion factor for length in rainfall rate

    NSPRN : int
        number of time steps to update the rainfall data

    NFQRN : int
        repetition frequency of the rainfall data

    DSSFL : str (max 50 characters)
        name of DSS file for data input

        Note
        ----
        writing precip data to a DSS file is currently not available

    Returns
    -------
    None
        writes output to file_object
    '''
    file_object.write(section_separator())
    file_object.write(centered_text('Rainfall Data Specifications'))
    file_object.write(blank_line())
    file_object.write(left_text('NRAIN ;  Number of rainfall stations (or pathnames if DSS files are used'))
    file_object.write(left_text('used in the model', leading_spaces=14))
    file_object.write(left_text('FACTRN;  Conversion factor for rainfall rate'))
    file_object.write(left_text('It is used to convert only the spatial component of the unit;', line_length=80, leading_spaces=14))
    file_object.write(left_text('DO NOT include the conversion factor for time component of the unit.', line_length=85, leading_spaces=14))
    file_object.write(left_text('* e.g. Unit of rainfall rate listed in this file = INCHES/MONTH', line_length=80, leading_spaces=14))
    file_object.write(left_text('Consistent unit used in simulation = FEET/DAY', line_length=80, leading_spaces=21))
    file_object.write(left_text('Enter FACTRN (INCHES/MONTH -> FEET/MONTH) = 8.33333E-02', line_length=80, leading_spaces=21))
    file_object.write(left_text('(conversion of MONTH -> DAY is performed automatically)', leading_spaces=22))
    file_object.write(left_text('NSPRN ;  Number of time steps to update the precipitation data'))
    file_object.write(left_text('* Enter any number if time-tracking option is on', leading_spaces=14))
    file_object.write(left_text('NFQRN ;  Repetition frequency of the precipitation data'))
    file_object.write(left_text('* Enter 0 if full time series data is supplied', leading_spaces=14))
    file_object.write(left_text('* Enter any number if time-tracking option is on', leading_spaces=14))
    file_object.write(left_text('DSSFL ;  The name of the DSS file for data input (maximum 50 characters);'))
    file_object.write(left_text('* Leave blank if DSS file is not used for data input")', leading_spaces=14))
    file_object.write(blank_line())
    file_object.write(section_separator(separator='-'))
    file_object.write(left_text('VALUE                                      DESCRIPTION', leading_spaces=10))
    file_object.write(section_separator(separator='-'))
    file_object.write('{:>15d}'.format(NRAIN) + ' ' * 38 + '\ NRAIN\n')
    file_object.write('{:>15.5f}'.format(FACTRN) + ' ' * 38 + '\ FACTRN\n')
    file_object.write('{:>15d}'.format(NSPRN) + ' ' * 38 + '\ NSPRN\n')
    file_object.write('{:>15d}'.format(NFQRN) + ' ' * 38 + '\ NSQRN\n')
    file_object.write('{:>52s}'.format(DSSFL) + ' ' + '\ DSSFL\n')
    file_object.write(section_separator())
    file_object.write(centered_text('Rainfall Data'))
    file_object.write(centered_text('(READ FROM THIS FILE'))
    file_object.write(blank_line())
    file_object.write(left_text('List the rainfall rates for each of the rainfall station below, if it will not be read from a DSS file (i.e. DSSFL is left blank above).'))
    file_object.write(blank_line())
    file_object.write(left_text('ITRN ;   Time'))
    file_object.write(left_text('ARAIN;   Rainfall rate at the corresponding rainfall station; [L/T]'))
    file_object.write(blank_line())
    file_object.write(section_separator(separator='-'))
    file_object.write(left_text('ITRN          ARAIN(1)  ARAIN(2)  ARAIN(3) ...'))
    file_object.write('C   TIME        {}\n'.format('{:>10}'*NRAIN).format(*[i+1 for i in range(NRAIN)]))
    file_object.write(section_separator(separator='-'))

def iwfm_file_header():
    '''
    Format file header to write to output text file
    '''
    string = section_separator() + \
             blank_line() + \
             centered_text('INTEGRATED WATER FLOW MODEL (IWFM)') + \
             centered_text('*** Version 2015 ***') + \
             blank_line()
    
    return string

def file_and_project_info(
    project_short_name, 
    project_long_name, 
    file_name, 
    version
):
    '''
    Format file and project information to write to output text file
    '''

    string = section_separator() + \
             blank_line() + \
             centered_text('PRECIPITATION DATA FILE') + \
             centered_text('Precipitation and Evapotranspiration Component') + \
             blank_line() + \
             'C             Project:  {}\n'.format(project_short_name) + \
             centered_text(project_long_name) + \
             'C             Filename: {}\n'.format(file_name) + \
             'C             Version:  {}\n'.format(version) + \
             blank_line()
    
    return string

def disclaimer(disclaimer_text):
    '''
    Format model disclaimer text to write to output text file
    '''
    string = section_separator() + \
             blank_line() + \
             centered_text('***** Model Disclaimer *****') + \
             blank_line() + \
             left_text(disclaimer_text) + \
             blank_line()

    return string

def license_and_contact_info(
    model_name, 
    organization,
    tech_support_email,
    contact_name,
    contact_email,
):
    '''
    Format software licensing information and technical support contact
    information to write to output text file
    '''
    
    year = datetime.datetime.today().year

    license = 'This model is free. You can redistribute it and/or modify it ' + \
              'under the terms of the GNU General Public License as published ' + \
              'by the Free Software Foundation; either version 2 of the License, ' + \
              'or (at your option) any later version.'

    warranty = 'This model is distributed WITHOUT ANY WARRANTY; without even ' + \
               'the implied warranty of MERCHANTABILITY or FITNESS FOR A ' + \
               'PARTICULAR PURPOSE.  See the GNU General Public License for ' + \
               'more details. (http://www.gnu.org/licenses/gpl.html)'

    license_contact = 'The GNU General Public License is available from the Free Software ' + \
                      'Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA  02111-1307, USA.'

    string = section_separator() + \
             blank_line() + \
             left_text(model_name, leading_spaces=3) + \
             left_text('Copyright (C) 2012-{}'.format(year), leading_spaces=3) + \
             left_text(organization, leading_spaces=3) + \
             blank_line() + \
             left_text(license, leading_spaces=3) + \
             blank_line() + \
             left_text(warranty, leading_spaces=3) + \
             blank_line() + \
             left_text(license_contact, leading_spaces=3) + \
             blank_line() + \
             left_text('For technical support, e-mail: {}'.format(tech_support_email), leading_spaces=3) + \
             blank_line() + \
             left_text('Principal Contact:', leading_spaces=3) + \
             left_text(contact_name, leading_spaces=11) + \
             left_text(contact_email, leading_spaces=11) + \
             blank_line() + \
             left_text('IWFM Contact:', leading_spaces=3) + \
             left_text('Emin Can Dogrul, PhD, PE, Senior Engineer, DWR', leading_spaces=11) + \
             left_text('(916) 654-7018, dogrul@water.ca.gov', leading_spaces=11) + \
             blank_line()

    return string
             
def file_description(description):
    '''
    Format file description to write to output text file

    Parameters
    ----------
    description : str
        description of output text file contents

    Returns
    -------
    str
        formatted string for writing to output text file
    '''
    string = section_separator() + \
             centered_text('File Description:') + \
             blank_line() + \
             left_text(description) + \
             blank_line()

    return string

def section_separator(comment_character='C', 
                      separator='*', 
                      line_length=80
):
    ''' 
    Generate section separator string

    Parameters
    ----------
    comment_character : str, default 'C'
        single character to denote comment

    Returns
    -------
    str
        string to separate sections of data or text
    '''
    if len(comment_character) != 1:
        raise ValueError('comment_character must be a single character')

    return comment_character + separator * (line_length - 1) + '\n'

def blank_line(comment_character='C'):
    '''
    Return blank line string

    Parameters
    ----------
    comment_character : str, default 'C'
        single character to denote comment

    Returns
    -------
    str
        string with new line character 
    '''
    if len(comment_character) != 1:
        raise ValueError('comment_character must be a single character')
    
    return '{}\n'.format(comment_character)

def centered_text(string, 
                  comment_character='C', 
                  line_length=80, 
                  leading_spaces=4
):
    '''
    Center and wrap long text strings to fit a desired line length.

    Recursively go through string to separate into multiple lines and format.

    Parameters
    ----------
    string : str
        string to be formatted

    comment_character : str, default 'C'
        single character to denote comment

    line_length : int, default 80
        max length of line before wrapping

    leading_spaces : int, default 4
        number of spaces before first character of string is written

    Returns
    -------
    str
        formatted string
    '''
    if not isinstance(comment_character, str):
        comment_character = str(comment_character)

    if len(comment_character) != 1:
        raise ValueError('comment_character must be a single character')

    # calculate length of string
    string_length = len(string)

    if string_length <= line_length - 2 * leading_spaces:

        # calculate number of spaces to add to center
        n_spaces = math.floor((line_length - string_length) / 2) - leading_spaces

        if leading_spaces == 0:
            return ' ' * n_spaces + string + '\n'

        return comment_character + ' ' * (leading_spaces - 1) + ' ' * n_spaces + string + '\n'

    else:
        
        space_list = []
        for i, c in enumerate(string):
            if c == ' ':
                space_list.append(i)
    
        last_space_position = 0
        for val in space_list:
            if val > line_length - 2 * leading_spaces:
                break
            last_space_position = val

        next_character = last_space_position + 1
    
        return centered_text(string[:last_space_position], 
                             comment_character,
                             line_length, 
                             leading_spaces) + \
               centered_text(string[next_character:], 
                             comment_character,
                             line_length, 
                             leading_spaces)

def left_text(string, 
              comment_character='C', 
              line_length=80, 
              leading_spaces=4
):
    '''
    Left-justify and wrap long text strings to fit a desired line length.

    Recursively go through string to separate into multiple lines and format.

    Parameters
    ----------
    string : str
        string to be formatted

    comment_character : str, default 'C'
        single character to denote comment

    line_length : int, default 80
        max length of line before wrapping

    leading_spaces : int, default 4
        number of spaces before first character of string is written

    Returns
    -------
    str
        formatted string
    '''
    if not isinstance(comment_character, str):
        comment_character = str(comment_character)

    if len(comment_character) != 1:
        raise ValueError('comment_character must be a single character')
    
    # calculate length of string
    string_length = len(string)

    if string_length <= line_length - leading_spaces:

        if leading_spaces == 0:
            return string + '\n'
           
        return comment_character + ' ' * (leading_spaces - 1) + string + '\n'

    else:
        
        space_list = []
        for i, c in enumerate(string):
            if c == ' ':
                space_list.append(i)
    
        last_space_position = 0
        for val in space_list:
            if val > line_length - leading_spaces:
                break
            last_space_position = val

        next_character = last_space_position + 1
    
        return left_text(string[:last_space_position], 
                         comment_character, 
                         line_length, 
                         leading_spaces) + \
               left_text(string[next_character:], 
                         comment_character, 
                         line_length, 
                         leading_spaces)


if __name__ == '__main__':
    project_short_name = 'C2VSim Fine Grid (C2VSimFG)'
    project_long_name = 'California Central Valley Groundwater-Surface Water Simulation Model'
    version = 'C2VSimFG_v1.01 - {}'.format(datetime.date.today())
    model_name = 'California Central Valley Groundwater-Surface Water Flow Model (C2VSim)'
    organization = 'State of California, Department of Water Resources'
    tech_support_email = 'c2vsimfgtechsupport@water.ca.gov'
    contact_name = 'Tyler Hatch, PhD, PE, Supervising Engineer, DWR'
    contact_email = 'tyler.hatch@water.ca.gov'

    d = 'This is Version 1.01 of C2VSimFG and is subject to change.  Users ' + \
        'of this version should be aware that this model is undergoing active ' + \
        'development and adjustment. Users of this model do so at their own ' + \
        'risk subject to the GNU General Public License below. The Department ' + \
        'does not guarantee the accuracy, completeness, or timeliness of the ' + \
        'information provided. Neither the Department of Water Resources nor ' + \
        'any of the sources of the information used by the Department in the ' + \
        'development of this model shall be responsible for any errors or ' + \
        'omissions, for the use, or results obtained from the use of this model.'

    with open('C2VSimFG_Precip.dat', 'w') as f:
        
        write_precip_header(
            f,
            project_short_name,
            project_long_name,
            version,
            organization,
            tech_support_email,
            contact_name,
            contact_email,
            model_name,
            d
        )

        write_precip_specs(f, 33561, 0.08333, 1, 0, '')